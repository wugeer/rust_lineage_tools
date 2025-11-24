use std::env;
use std::fs;
use std::io::{self, Read};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let mut args: Vec<String> = env::args().skip(1).collect();

    // Check if running in CLI mode
    let cli_mode = if let Some(pos) = args.iter().position(|a| a == "--cli") {
        args.remove(pos);
        true
    } else {
        false
    };

    if cli_mode {
        // Run in original CLI mode
        run_cli_mode(args);
        Ok(())
    } else {
        // Run as HTTP server
        run_server_mode().await
    }
}

async fn run_server_mode() -> std::io::Result<()> {
    // Load configuration
    let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
    let config =
        hive_lineage::config::AppConfig::from_file(&config_path).map_err(std::io::Error::other)?;

    log::info!("Loaded configuration from {}", config_path);

    // Create database pool with configured size
    let database_url = config.database_url();
    let pool_size = config.database.pool_size as u32;
    let pool =
        hive_lineage::db::create_pool(&database_url, pool_size).map_err(std::io::Error::other)?;

    log::info!(
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
