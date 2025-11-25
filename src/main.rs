use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut args: Vec<String> = env::args().skip(1).collect();

    // Check if running in CLI mode
    let cli_mode = if let Some(pos) = args.iter().position(|a| a == "--cli") {
        args.remove(pos);
        true
    } else {
        false
    };

    if cli_mode {
        // Run in original CLI mode (simple stdout logging)
        run_cli_mode(args);
        Ok(())
    } else {
        // Run as HTTP server (with file logging)
        run_server_mode().await
    }
}

async fn run_server_mode() -> std::io::Result<()> {
    // Load configuration
    let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
    let config =
        hive_lineage::config::AppConfig::from_file(&config_path).map_err(std::io::Error::other)?;

    // Initialize logging
    let writer_state = init_logging(&config.logging).map_err(std::io::Error::other)?;
    start_log_maintenance(writer_state);

    tracing::info!("Loaded configuration from {}", config_path);

    // Create database pool with configured size
    let database_url = config.database_url();
    let pool_size = config.database.pool_size as u32;
    let pool =
        hive_lineage::db::create_pool(&database_url, pool_size).map_err(std::io::Error::other)?;

    tracing::info!(
        "Database connection pool created with max_size={}",
        pool_size
    );

    // Note: Diesel migrations should be run manually using diesel CLI:
    // diesel migration run
    // Start server with configured workers
    hive_lineage::server::start_server(
        &config.server.host,
        config.server.port,
        config.server.worker_threads,
        pool,
    )
    .await
}

fn init_logging(
    config: &hive_lineage::config::LogConfig,
) -> anyhow::Result<Arc<Mutex<WriterState>>> {
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    // Create log directory if it doesn't exist
    std::fs::create_dir_all(&config.log_dir)?;

    let (file_writer, writer_state) = SharedLogWriter::new(&config.log_dir, config.max_log_files)?;
    // Clean up old log files
    cleanup_old_logs(&config.log_dir, config.max_log_files)?;

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_writer);

    // Parse log level
    let log_level = config
        .log_level
        .parse::<tracing::Level>()
        .unwrap_or(tracing::Level::INFO);

    // Build the subscriber with both stdout and file output
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level.to_string()));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().with_writer(std::io::stdout))
        .with(fmt::layer().with_writer(non_blocking).with_ansi(false))
        .init();

    // Leak the guard to keep it alive for the entire program lifetime
    std::mem::forget(_guard);

    Ok(writer_state)
}

struct SharedLogWriter {
    state: Arc<Mutex<WriterState>>,
}

impl SharedLogWriter {
    fn new(log_dir: &str, max_files: usize) -> anyhow::Result<(Self, Arc<Mutex<WriterState>>)> {
        let state = WriterState::new(log_dir, max_files)?;
        let arc = Arc::new(Mutex::new(state));
        Ok((Self { state: arc.clone() }, arc))
    }
}

impl Write for SharedLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "log writer poisoned"))?;
        state
            .ensure_current()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        state.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "log writer poisoned"))?;
        state.file.flush()
    }
}

struct WriterState {
    log_dir: String,
    active_path: PathBuf,
    file: std::fs::File,
    current_date: chrono::NaiveDate,
    max_files: usize,
}

impl WriterState {
    fn new(log_dir: &str, max_files: usize) -> anyhow::Result<Self> {
        std::fs::create_dir_all(log_dir)?;
        let active_path = Path::new(log_dir).join("hive_lineage.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        let today = chrono::Local::now().date_naive();
        let mut state = Self {
            log_dir: log_dir.to_string(),
            active_path,
            file,
            current_date: today,
            max_files,
        };
        state.initialize_existing_file()?;
        Ok(state)
    }

    fn initialize_existing_file(&mut self) -> anyhow::Result<()> {
        if !self.active_path.exists() {
            return Ok(());
        }
        let metadata = std::fs::metadata(&self.active_path)?;
        if metadata.len() == 0 {
            return Ok(());
        }
        if let Ok(modified) = metadata.modified() {
            let modified_date = chrono::DateTime::<chrono::Local>::from(modified).date_naive();
            self.current_date = modified_date;
        }
        let today = chrono::Local::now().date_naive();
        if self.current_date < today {
            self.rotate_to(today)?;
        }
        Ok(())
    }

    fn ensure_current(&mut self) -> anyhow::Result<()> {
        let today = chrono::Local::now().date_naive();
        if today != self.current_date {
            self.rotate_to(today)?;
        }
        Ok(())
    }

    fn rotate_to(&mut self, new_date: chrono::NaiveDate) -> anyhow::Result<()> {
        use std::fs::OpenOptions;

        if !self.active_path.exists() {
            self.file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.active_path)?;
            self.current_date = new_date;
            return Ok(());
        }

        self.file.flush()?;
        let archive_path = Path::new(&self.log_dir).join(format!(
            "hive_lineage.log.{}",
            self.current_date.format("%Y-%m-%d")
        ));
        std::fs::rename(&self.active_path, &archive_path)?;
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.active_path)?;
        let old_file = std::mem::replace(&mut self.file, new_file);
        drop(old_file);

        compress_log_file(&archive_path)?;
        cleanup_old_logs(&self.log_dir, self.max_files)?;

        self.current_date = new_date;
        Ok(())
    }
}

fn cleanup_old_logs(log_dir: &str, max_files: usize) -> anyhow::Result<()> {
    use std::fs;
    use std::path::Path;

    let log_path = Path::new(log_dir);
    if !log_path.exists() {
        return Ok(());
    }

    // Get today's date for comparison
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let current_log_name = format!("hive_lineage.log.{}", today);

    // Compress old uncompressed log files (not today's)
    for entry in fs::read_dir(log_path)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            // Compress old dated log files (but not today's log and not already .gz)
            if filename.starts_with("hive_lineage.log.")
                && filename != current_log_name
                && !filename.ends_with(".gz")
            {
                if let Err(e) = compress_log_file(&path) {
                    eprintln!("Failed to compress log file {:?}: {}", path, e);
                } else {
                    eprintln!("Compressed log file: {:?}", path);
                }
            }
        }
    }

    // Collect all log files (both .log and .log.gz) with their metadata
    let mut log_files: Vec<_> = fs::read_dir(log_path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("hive_lineage.log."))
                .unwrap_or(false)
        })
        .filter_map(|entry| {
            let metadata = entry.metadata().ok()?;
            let modified = metadata.modified().ok()?;
            Some((entry.path(), modified))
        })
        .collect();

    // Sort by modification time (newest first)
    log_files.sort_by(|a, b| b.1.cmp(&a.1));

    // Remove files beyond max_files limit
    for (path, _) in log_files.iter().skip(max_files) {
        if let Err(e) = fs::remove_file(path) {
            eprintln!("Failed to remove old log file {:?}: {}", path, e);
        } else {
            println!("Removed old log file: {:?}", path);
        }
    }

    Ok(())
}

fn compress_log_file(path: &std::path::Path) -> anyhow::Result<()> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::fs::File;
    use std::io::{Read, Write};

    // Read the original file
    let mut input_file = File::open(path)?;
    let mut buffer = Vec::new();
    input_file.read_to_end(&mut buffer)?;

    // Create compressed file by appending .gz to the original filename
    let mut gz_path = path.as_os_str().to_owned();
    gz_path.push(".gz");
    let output_file = File::create(&gz_path)?;
    let mut encoder = GzEncoder::new(output_file, Compression::default());
    encoder.write_all(&buffer)?;
    encoder.finish()?;

    // Remove original uncompressed file
    std::fs::remove_file(path)?;

    Ok(())
}

fn start_log_maintenance(writer_state: Arc<Mutex<WriterState>>) {
    use tokio::time::{sleep, Duration};

    actix_web::rt::spawn(async move {
        loop {
            sleep(Duration::from_secs(60)).await;
            let mut state = match writer_state.lock() {
                Ok(guard) => guard,
                Err(_) => {
                    tracing::warn!("Log writer state poisoned");
                    continue;
                }
            };
            if let Err(e) = state.ensure_current() {
                tracing::warn!("Failed to rotate active log file: {}", e);
            }
        }
    });
}

fn run_cli_mode(mut args: Vec<String>) {
    let tables_mode = if let Some(pos) = args.iter().position(|a| a == "--tables" || a == "-t") {
        args.remove(pos);
        true
    } else {
        false
    };
    let pretty = if let Some(pos) = args.iter().position(|a| a == "--pretty" || a == "-p") {
        args.remove(pos);
        true
    } else {
        false
    };
    let json_mode = if let Some(pos) = args.iter().position(|a| a == "--json" || a == "-j") {
        args.remove(pos);
        true
    } else {
        false
    };

    let res = if args.is_empty() || (args.len() == 1 && args[0] == "-") {
        // Read from stdin
        let mut buf = String::new();
        if let Err(e) = io::stdin().read_to_string(&mut buf) {
            eprintln!("Failed to read stdin: {}", e);
            std::process::exit(2);
        }
        run_once(&buf, tables_mode, json_mode, pretty)
    } else {
        for path in &args {
            match fs::read_to_string(path) {
                Ok(contents) => {
                    if let Err(e) = run_once(&contents, tables_mode, json_mode, pretty) {
                        eprintln!("Error in {}: {}", path, e);
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read {}: {}", path, e);
                    std::process::exit(2);
                }
            }
        }
        Ok(())
    };
    if let Err(e) = res {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run_once(sql: &str, tables_mode: bool, json_mode: bool, pretty: bool) -> anyhow::Result<()> {
    if tables_mode {
        if json_mode {
            let infos = hive_lineage::analyze_sql_tables_detailed(sql)?;
            if pretty {
                println!("{}", serde_json::to_string_pretty(&infos)?);
            } else {
                println!("{}", serde_json::to_string(&infos)?);
            }
        } else {
            let lines = hive_lineage::analyze_sql_tables(sql)?;
            for l in lines {
                println!("{}", l);
            }
        }
    } else if json_mode {
        let infos = hive_lineage::analyze_sql_lineage_detailed(sql)?;
        if pretty {
            println!("{}", serde_json::to_string_pretty(&infos)?);
        } else {
            println!("{}", serde_json::to_string(&infos)?);
        }
    } else {
        let lines = hive_lineage::analyze_sql_lineage(sql)?;
        for l in lines {
            println!("{}", l);
        }
    }
    Ok(())
}
