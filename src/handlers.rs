use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::db::DbPool;

/// Request model for lineage analysis
#[derive(Debug, Deserialize)]
pub struct LineageRequest {
    pub sql: String,
    pub dag_id: String,
    pub task_id: String,
}

/// Response model for table lineage
#[derive(Debug, Serialize)]
pub struct TableLineageResponse {
    #[serde(flatten)]
    pub lineages: HashMap<String, String>,
}

/// Response model for field lineage
#[derive(Debug, Serialize)]
pub struct FieldLineageResponse {
    #[serde(flatten)]
    pub lineages: HashMap<String, HashMap<String, Vec<String>>>,
}

/// Error response model
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Handler for table lineage analysis
pub async fn analyze_table_lineage(
    pool: web::Data<DbPool>,
    req: web::Json<LineageRequest>,
) -> impl Responder {
    tracing::info!(
        "Analyzing table lineage for dag_id={}, task_id={}",
        req.dag_id,
        req.task_id
    );

    // Analyze SQL to get structured table lineage data
    let stmt_infos = match crate::analyze_sql_tables_detailed(&req.sql) {
        Ok(infos) => infos,
        Err(e) => {
            tracing::error!("SQL analysis failed: {}", e);
            return HttpResponse::BadRequest().json(ErrorResponse {
                error: format!("SQL analysis failed: {}", e),
            });
        }
    };

    // Convert to response format and insert into database
    let mut lineages = HashMap::new();

    for info in stmt_infos {
        let target = info.target;
        let sources = info.sources.join(", ");

        // Insert into database
        let mut conn = match pool.get() {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("Failed to get database connection: {}", e);
                return HttpResponse::InternalServerError().json(ErrorResponse {
                    error: "Database connection error".to_string(),
                });
            }
        };

        if let Err(e) =
            crate::db::insert_table_lineage(&mut conn, &req.dag_id, &req.task_id, &target, &sources)
        {
            tracing::error!("Failed to insert table lineage: {}", e);
            return HttpResponse::InternalServerError().json(ErrorResponse {
                error: "Failed to save lineage data".to_string(),
            });
        }

        lineages.insert(target, sources);
    }

    HttpResponse::Ok().json(TableLineageResponse { lineages })
}

/// Handler for field lineage analysis
pub async fn analyze_field_lineage(
    pool: web::Data<DbPool>,
    req: web::Json<LineageRequest>,
) -> impl Responder {
    tracing::info!(
        "Analyzing field lineage for dag_id={}, task_id={}",
        req.dag_id,
        req.task_id
    );

    // Analyze SQL to get structured field lineage data
    let col_infos = match crate::analyze_sql_lineage_detailed(&req.sql) {
        Ok(infos) => infos,
        Err(e) => {
            tracing::error!("SQL analysis failed: {}", e);
            return HttpResponse::BadRequest().json(ErrorResponse {
                error: format!("SQL analysis failed: {}", e),
            });
        }
    };

    // Convert to response format and insert into database
    let mut lineages: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

    for info in col_infos {
        let target_table = info.target;
        let target_field = info.column;

        // Get source fields or expression
        let source_fields = match info.sources {
            Some(sources) => sources,
            None => {
                // For constant expressions, use the expr field
                if let Some(expr) = info.expr {
                    vec![expr]
                } else {
                    continue; // Skip if neither sources nor expr is available
                }
            }
        };

        // Insert into database
        let mut conn = match pool.get() {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("Failed to get database connection: {}", e);
                return HttpResponse::InternalServerError().json(ErrorResponse {
                    error: "Database connection error".to_string(),
                });
            }
        };

        if let Err(e) = crate::db::insert_field_lineage(
            &mut conn,
            &req.dag_id,
            &req.task_id,
            &target_table,
            &target_field,
            &source_fields,
        ) {
            tracing::error!("Failed to insert field lineage: {}", e);
            return HttpResponse::InternalServerError().json(ErrorResponse {
                error: "Failed to save lineage data".to_string(),
            });
        }

        // Add to response
        lineages
            .entry(target_table)
            .or_default()
            .insert(target_field, source_fields);
    }

    HttpResponse::Ok().json(FieldLineageResponse { lineages })
}

/// Health check handler
pub async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("OK")
}
