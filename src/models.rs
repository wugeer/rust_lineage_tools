use chrono::NaiveDateTime;
use diesel::prelude::*;

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::table_lineage)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TableLineage {
    pub id: i32,
    pub dag_id: String,
    pub task_id: String,
    pub target_table: String,
    pub source_tables: String,
    pub created_at: NaiveDateTime,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = crate::schema::table_lineage)]
pub struct NewTableLineage {
    pub dag_id: String,
    pub task_id: String,
    pub target_table: String,
    pub source_tables: String,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::field_lineage)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct FieldLineage {
    pub id: i32,
    pub dag_id: String,
    pub task_id: String,
    pub target_table: String,
    pub target_field: String,
    pub source_fields: String,
    pub created_at: NaiveDateTime,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = crate::schema::field_lineage)]
pub struct NewFieldLineage {
    pub dag_id: String,
    pub task_id: String,
    pub target_table: String,
    pub target_field: String,
    pub source_fields: String,
}
