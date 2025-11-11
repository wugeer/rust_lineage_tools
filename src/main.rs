use std::env;
use std::fs;
use std::io::{self, Read};

fn main() {
    let mut args: Vec<String> = env::args().skip(1).collect();
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
        run_one(&buf, tables_mode, json_mode, pretty)
    } else {
        for path in &args {
            match fs::read_to_string(path) {
                Ok(contents) => {
                    if let Err(e) = run_one(&contents, tables_mode, json_mode, pretty) {
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

fn run_one(sql: &str, tables_mode: bool, json_mode: bool, pretty: bool) -> anyhow::Result<()> {
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
