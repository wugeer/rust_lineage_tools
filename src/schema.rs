// @generated automatically by Diesel CLI.

diesel::table! {
    field_lineage (id) {
        id -> Int4,
        dag_id -> Varchar,
        task_id -> Varchar,
        target_table -> Varchar,
        target_field -> Varchar,
        source_fields -> Text,
        created_at -> Timestamp,
    }
}

diesel::table! {
    table_lineage (id) {
        id -> Int4,
        dag_id -> Varchar,
        task_id -> Varchar,
        target_table -> Varchar,
        source_tables -> Text,
        created_at -> Timestamp,
    }
}

diesel::allow_tables_to_appear_in_same_query!(field_lineage, table_lineage,);
