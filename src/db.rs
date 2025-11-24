use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager, Pool, PooledConnection};

pub type DbPool = Pool<ConnectionManager<PgConnection>>;
pub type DbConn = PooledConnection<ConnectionManager<PgConnection>>;

use crate::models::{NewFieldLineage, NewTableLineage};
use crate::schema::{field_lineage, table_lineage};

/// Create database connection pool with configurable size
pub fn create_pool(database_url: &str, max_size: u32) -> Result<DbPool, r2d2::PoolError> {
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    Pool::builder().max_size(max_size).build(manager)
}

/// Insert table lineage record (always creates new record)
pub fn insert_table_lineage(
    conn: &mut PgConnection,
    dag_id: &str,
    task_id: &str,
    target_table: &str,
    source_tables: &str,
) -> Result<usize, diesel::result::Error> {
    let new_lineage = NewTableLineage {
        dag_id: dag_id.to_string(),
        task_id: task_id.to_string(),
        target_table: target_table.to_string(),
        source_tables: source_tables.to_string(),
    };

    diesel::insert_into(table_lineage::table)
        .values(&new_lineage)
        .execute(conn)
}

/// Insert field lineage record (always creates new record)
pub fn insert_field_lineage(
    conn: &mut PgConnection,
    dag_id: &str,
    task_id: &str,
    target_table: &str,
    target_field: &str,
    source_fields: &[String],
) -> Result<usize, diesel::result::Error> {
    // Convert array to comma-separated string
    let source_fields_str = source_fields.join(", ");

    let new_lineage = NewFieldLineage {
        dag_id: dag_id.to_string(),
        task_id: task_id.to_string(),
        target_table: target_table.to_string(),
        target_field: target_field.to_string(),
        source_fields: source_fields_str,
    };

    diesel::insert_into(field_lineage::table)
        .values(&new_lineage)
        .execute(conn)
}
