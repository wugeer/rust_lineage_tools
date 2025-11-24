-- Create table_lineage table
CREATE TABLE public.table_lineage (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    target_table VARCHAR(500) NOT NULL,
    source_tables TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create field_lineage table
CREATE TABLE public.field_lineage (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    target_table VARCHAR(500) NOT NULL,
    target_field VARCHAR(255) NOT NULL,
    source_fields text NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
