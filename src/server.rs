use actix_web::{middleware, web, App, HttpServer};
use std::io;

use crate::db::DbPool;
use crate::handlers::{analyze_field_lineage, analyze_table_lineage, health_check};

/// Start the HTTP server
pub async fn start_server(host: &str, port: u16, workers: usize, pool: DbPool) -> io::Result<()> {
    let bind_addr = format!("{}:{}", host, port);

    log::info!("Starting server on {} with {} workers", bind_addr, workers);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .wrap(middleware::Logger::default())
            .route("/health", web::get().to(health_check))
            .route("/table-lineage", web::post().to(analyze_table_lineage))
            .route("/field-lineage", web::post().to(analyze_field_lineage))
    })
    .workers(workers)
    .bind(&bind_addr)?
    .run()
    .await
}
