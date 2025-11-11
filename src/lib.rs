use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use serde::Serialize;
use sqlparser::ast;
use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem, SetExpr, Spanned,
    Statement, TableAlias, TableFactor, TableObject, TableWithJoins, Use,
};
use sqlparser::dialect::HiveDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;
use std::collections::{BTreeSet, HashMap};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
struct TableKey {
    db: String,
    table: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
struct ColumnRef {
    table: TableKey,
    column: String,
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.table.db, self.table.table, self.column)
    }
}

#[derive(Debug, Clone)]
struct TargetCol {
    table: TableKey,
    column: String,
}

impl fmt::Display for TargetCol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.table.db, self.table.table, self.column)
    }
}

#[derive(Debug, Default)]
struct Context {
    current_db: Option<String>,
    // Known schemas: (db, table) -> columns in order.
    schemas: HashMap<TableKey, Vec<String>>,
}

#[derive(Debug, Default, Clone)]
struct CteInfo {
    columns: Vec<String>,
    // column name -> resolved physical column sources
    sources: HashMap<String, BTreeSet<ColumnRef>>,
    // column name -> original expression string when there are no sources
    exprs: HashMap<String, String>,
}

#[derive(Debug, Default, Clone)]
struct CteDefs {
    // cte name -> info
    defs: HashMap<String, CteInfo>,
}

#[derive(Debug, Default, Clone)]
struct DerivedInfo {
    columns: Vec<String>,
    // name-based lookup (may overwrite on duplicate names)
    sources: HashMap<String, BTreeSet<ColumnRef>>,
    exprs: HashMap<String, String>,
    // index-preserving slots to handle duplicate output names
    sources_by_index: Vec<Option<BTreeSet<ColumnRef>>>,
    exprs_by_index: Vec<Option<String>>,
}

#[derive(Debug, Clone)]
struct ExpandedItem {
    item: SelectItem,
    out_name: String,
    is_from_wildcard: bool,
}

/// Analyze Hive SQL script and produce column-level lineage lines.
/// Returns lines like: `db.target_table.col <- db.source_table.col1, db.source_table.col2`.
pub fn analyze_sql_lineage(sql: &str) -> Result<Vec<String>> {
    let dialect = HiveDialect {};
    let mut ctx = Context::default();
    // Parse entire script; sqlparser supports multiple statements separated by `;`.
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| anyhow!(e.to_string()))?;

    let mut lines: Vec<String> = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::Use(u) => {
                if let Some(db) = use_to_db(&u)? {
                    ctx.current_db = Some(db);
                }
            }
            Statement::CreateTable(ct) => {
                let name = &ct.name;
                let columns = &ct.columns;
                let query = &ct.query;
                // Record schema if explicit cols provided
                if !columns.is_empty() {
                    let (db, table) = qualify_table(&ctx, name)?;
                    let key = TableKey { db, table };
                    let cols = columns.iter().map(|c| ident_to_string(&c.name)).collect();
                    ctx.schemas.insert(key, cols);
                }

                // Handle CTAS
                if let Some(q) = query.as_ref() {
                    let (db, table) = qualify_table(&ctx, name)?;
                    let target_table = TableKey { db, table };

                    let select = expect_simple_select(q)?;
                    let cte_defs = build_cte_defs(&ctx, q)?;
                    let info = analyze_select_info(&ctx, select, &cte_defs)?;
                    for (i, col) in info.columns.iter().enumerate() {
                        let target_col = TargetCol {
                            table: target_table.clone(),
                            column: col.clone(),
                        };
                        if let Some(Some(srcs)) = info.sources_by_index.get(i) {
                            lines.push(format!(
                                "{} <- {}",
                                target_col,
                                srcs.iter().map(|c| c.to_string()).sorted().join(", ")
                            ));
                        } else if let Some(Some(expr)) = info.exprs_by_index.get(i) {
                            lines.push(format!("{} <- {}", target_col, expr));
                        }
                    }
                    ctx.schemas
                        .entry(target_table)
                        .or_insert(info.columns.clone());
                }
            }
            Statement::Insert(ins) => {
                // Support INSERT [OVERWRITE] [TABLE] <name> [(columns...)] SELECT ...
                // The `overwrite` flag distinguishes INSERT INTO vs INSERT OVERWRITE TABLE.
                let (db, table) = match &ins.table {
                    TableObject::TableName(name) => qualify_table(&ctx, name)?,
                    TableObject::TableFunction(_) => {
                        bail!("Unsupported INSERT target: table function")
                    }
                };
                let target_table = TableKey { db, table };

                // Determine target column names: explicit list, else known schema.
                let target_cols: Vec<String> = if !ins.columns.is_empty() {
                    ins.columns.iter().map(ident_to_string).collect()
                } else if let Some(cols) = ctx.schemas.get(&target_table) {
                    cols.clone()
                } else {
                    // Fall back to using projection output names from SELECT by resolving it first
                    let source_q = ins
                        .source
                        .as_ref()
                        .ok_or_else(|| anyhow!("INSERT missing source SELECT"))?;
                    let select = expect_simple_select(source_q)?;
                    let cte_defs = build_cte_defs(&ctx, source_q)?;
                    let info = analyze_select_info(&ctx, select, &cte_defs)?;
                    info.columns.clone()
                };

                let source_q = ins
                    .source
                    .as_ref()
                    .ok_or_else(|| anyhow!("INSERT missing source SELECT"))?;
                let select = expect_simple_select(source_q)?;
                let cte_defs = build_cte_defs(&ctx, source_q)?;
                let info = analyze_select_info(&ctx, select, &cte_defs)?;
                if target_cols.len() != info.columns.len() {
                    bail!(
                        "Target columns count ({}) does not match SELECT projection ({}) for table {}.{}",
                        target_cols.len(),
                        info.columns.len(),
                        target_table.db,
                        target_table.table
                    );
                }

                for (i, _col) in info.columns.iter().enumerate() {
                    let target_col = TargetCol {
                        table: target_table.clone(),
                        column: target_cols[i].clone(),
                    };
                    if let Some(Some(srcs)) = info.sources_by_index.get(i) {
                        lines.push(format!(
                            "{} <- {}",
                            target_col,
                            srcs.iter().map(|c| c.to_string()).sorted().join(", ")
                        ));
                    } else if let Some(Some(expr)) = info.exprs_by_index.get(i) {
                        lines.push(format!("{} <- {}", target_col, expr));
                    }
                }
            }
            Statement::CreateView {
                name,
                columns,
                query,
                ..
            } => {
                let (db, table) = qualify_table(&ctx, &name)?;
                let target_table = TableKey { db, table };
                let select = expect_simple_select(&query)?;
                let cte_defs = build_cte_defs(&ctx, &query)?;
                let info = analyze_select_info(&ctx, select, &cte_defs)?;
                let target_cols: Vec<String> = if !columns.is_empty() {
                    columns.iter().map(|c| ident_to_string(&c.name)).collect()
                } else {
                    info.columns.clone()
                };

                if target_cols.len() != info.columns.len() {
                    bail!(
                        "Target columns count ({}) does not match SELECT projection ({}) for view {}.{}",
                        target_cols.len(),
                        info.columns.len(),
                        target_table.db,
                        target_table.table
                    );
                }
                for (i, _col) in info.columns.iter().enumerate() {
                    let target_col = TargetCol {
                        table: target_table.clone(),
                        column: target_cols[i].clone(),
                    };
                    if let Some(Some(srcs)) = info.sources_by_index.get(i) {
                        lines.push(format!(
                            "{} <- {}",
                            target_col,
                            srcs.iter().map(|c| c.to_string()).sorted().join(", ")
                        ));
                    } else if let Some(Some(expr)) = info.exprs_by_index.get(i) {
                        lines.push(format!("{} <- {}", target_col, expr));
                    }
                }
                ctx.schemas.entry(target_table).or_insert(target_cols);
            }
            // Record CREATE VIEW schema? Not required.
            // Other statements: ignore.
            _ => {}
        }
    }

    Ok(lines)
}

/// Analyze each relevant statement and output table-level lineage per statement.
/// Ignored statements: USE/SET, plain CREATE TABLE DDL (without query), etc.
/// For CTAS/INSERT/CREATE VIEW, emits lines like: `db.target_table <- db.source1, db.source2` per statement.
pub fn analyze_sql_tables(sql: &str) -> Result<Vec<String>> {
    let dialect = HiveDialect {};
    let mut ctx = Context::default();
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| anyhow!(e.to_string()))?;
    let mut lines: Vec<String> = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::Use(u) => {
                if let Some(db) = use_to_db(&u)? {
                    ctx.current_db = Some(db);
                }
            }
            Statement::Set(_) => {}
            Statement::CreateTable(ct) => {
                // record schema if columns present
                if !ct.columns.is_empty() {
                    let (db, table) = qualify_table(&ctx, &ct.name)?;
                    let key = TableKey { db, table };
                    let cols = ct
                        .columns
                        .iter()
                        .map(|c| ident_to_string(&c.name))
                        .collect();
                    ctx.schemas.insert(key, cols);
                }
                if let Some(q) = ct.query.as_ref() {
                    // CTAS: compute sources
                    let (tdb, ttable) = qualify_table(&ctx, &ct.name)?;
                    let target = TableKey {
                        db: tdb,
                        table: ttable,
                    };
                    let select = expect_simple_select(q)?;
                    let cte_defs = build_cte_defs(&ctx, q)?;
                    let (alias_map, cte_aliases, derived_aliases) =
                        build_alias_map_with_cte(&ctx, &select.from, &cte_defs)?;
                    let sources = collect_base_tables_in_scope(
                        &alias_map,
                        &cte_aliases,
                        &derived_aliases,
                        &cte_defs,
                    );
                    let s = sources
                        .iter()
                        .map(|t| format!("{}.{}", t.db, t.table))
                        .sorted()
                        .join(", ");
                    lines.push(format!("{}.{} <- {}", target.db, target.table, s));
                }
            }
            Statement::Insert(ins) => {
                let (db, table) = match &ins.table {
                    TableObject::TableName(name) => qualify_table(&ctx, name)?,
                    TableObject::TableFunction(_) => continue,
                };
                let target = TableKey { db, table };
                if let Some(source_q) = ins.source.as_ref() {
                    let select = expect_simple_select(source_q)?;
                    let cte_defs = build_cte_defs(&ctx, source_q)?;
                    let (alias_map, cte_aliases, derived_aliases) =
                        build_alias_map_with_cte(&ctx, &select.from, &cte_defs)?;
                    let sources = collect_base_tables_in_scope(
                        &alias_map,
                        &cte_aliases,
                        &derived_aliases,
                        &cte_defs,
                    );
                    let s = sources
                        .iter()
                        .map(|t| format!("{}.{}", t.db, t.table))
                        .sorted()
                        .join(", ");
                    lines.push(format!("{}.{} <- {}", target.db, target.table, s));
                }
            }
            Statement::CreateView {
                name,
                query,
                columns,
                ..
            } => {
                // columns ignored for table-level lineage; compute sources
                let (db, table) = qualify_table(&ctx, &name)?;
                let target = TableKey { db, table };
                let select = expect_simple_select(&query)?;
                let cte_defs = build_cte_defs(&ctx, &query)?;
                let (alias_map, cte_aliases, derived_aliases) =
                    build_alias_map_with_cte(&ctx, &select.from, &cte_defs)?;
                let sources = collect_base_tables_in_scope(
                    &alias_map,
                    &cte_aliases,
                    &derived_aliases,
                    &cte_defs,
                );
                let s = sources
                    .iter()
                    .map(|t| format!("{}.{}", t.db, t.table))
                    .sorted()
                    .join(", ");
                lines.push(format!("{}.{} <- {}", target.db, target.table, s));
                // store view schema if provided
                if !columns.is_empty() {
                    let cols = columns.iter().map(|c| ident_to_string(&c.name)).collect();
                    ctx.schemas.insert(target, cols);
                }
            }
            _ => {}
        }
    }
    Ok(lines)
}

#[derive(Debug, Clone, Serialize)]
pub struct TableStmtInfo {
    pub stmt_type: String,
    pub start_line: u32,
    pub start_col: u32,
    pub end_line: u32,
    pub end_col: u32,
    pub target: String,
    pub sources: Vec<String>,
    pub statement: String,
}

/// Detailed per-statement table lineage with statement type and span positions.
pub fn analyze_sql_tables_detailed(sql: &str) -> Result<Vec<TableStmtInfo>> {
    let dialect = HiveDialect {};
    let mut ctx = Context::default();
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| anyhow!(e.to_string()))?;
    let mut out: Vec<TableStmtInfo> = Vec::new();

    for stmt in statements {
        let span = stmt.span();
        match stmt {
            Statement::Use(u) => {
                if let Some(db) = use_to_db(&u)? {
                    ctx.current_db = Some(db);
                }
            }
            Statement::Set(_) => {}
            Statement::CreateTable(ct) => {
                if !ct.columns.is_empty() {
                    let (db, table) = qualify_table(&ctx, &ct.name)?;
                    let key = TableKey { db, table };
                    let cols = ct
                        .columns
                        .iter()
                        .map(|c| ident_to_string(&c.name))
                        .collect();
                    ctx.schemas.insert(key, cols);
                }
                if let Some(q) = ct.query.as_ref() {
                    let (tdb, ttable) = qualify_table(&ctx, &ct.name)?;
                    let target = TableKey {
                        db: tdb,
                        table: ttable,
                    };
                    let select = expect_simple_select(q)?;
                    let cte_defs = build_cte_defs(&ctx, q)?;
                    let (alias_map, cte_aliases, derived_aliases) =
                        build_alias_map_with_cte(&ctx, &select.from, &cte_defs)?;
                    let sources = collect_base_tables_in_scope(
                        &alias_map,
                        &cte_aliases,
                        &derived_aliases,
                        &cte_defs,
                    );
                    let mut srcs: Vec<String> = sources
                        .iter()
                        .map(|t| format!("{}.{}", t.db, t.table))
                        .collect();
                    srcs.sort();
                    out.push(TableStmtInfo {
                        stmt_type: "CTAS".to_string(),
                        start_line: span.start.line as u32,
                        start_col: span.start.column as u32,
                        end_line: span.end.line as u32,
                        end_col: span.end.column as u32,
                        target: format!("{}.{}", target.db, target.table),
                        sources: srcs,
                        statement: extract_snippet(sql, span),
                    });
                }
            }
            Statement::Insert(ins) => {
                let (db, table) = match &ins.table {
                    TableObject::TableName(name) => qualify_table(&ctx, name)?,
                    TableObject::TableFunction(_) => continue,
                };
                let target = TableKey { db, table };
                if let Some(source_q) = ins.source.as_ref() {
                    let select = expect_simple_select(source_q)?;
                    let cte_defs = build_cte_defs(&ctx, source_q)?;
                    let (alias_map, cte_aliases, derived_aliases) =
                        build_alias_map_with_cte(&ctx, &select.from, &cte_defs)?;
                    let sources = collect_base_tables_in_scope(
                        &alias_map,
                        &cte_aliases,
                        &derived_aliases,
                        &cte_defs,
                    );
                    let mut srcs: Vec<String> = sources
                        .iter()
                        .map(|t| format!("{}.{}", t.db, t.table))
                        .collect();
                    srcs.sort();
                    out.push(TableStmtInfo {
                        stmt_type: "INSERT".to_string(),
                        start_line: span.start.line as u32,
                        start_col: span.start.column as u32,
                        end_line: span.end.line as u32,
                        end_col: span.end.column as u32,
                        target: format!("{}.{}", target.db, target.table),
                        sources: srcs,
                        statement: extract_snippet(sql, span),
                    });
                }
            }
            Statement::CreateView {
                name,
                query,
                columns,
                ..
            } => {
                let (db, table) = qualify_table(&ctx, &name)?;
                let target = TableKey { db, table };
                let select = expect_simple_select(&query)?;
                let cte_defs = build_cte_defs(&ctx, &query)?;
                let (alias_map, cte_aliases, derived_aliases) =
                    build_alias_map_with_cte(&ctx, &select.from, &cte_defs)?;
                let sources = collect_base_tables_in_scope(
                    &alias_map,
                    &cte_aliases,
                    &derived_aliases,
                    &cte_defs,
                );
                let mut srcs: Vec<String> = sources
                    .iter()
                    .map(|t| format!("{}.{}", t.db, t.table))
                    .collect();
                srcs.sort();
                out.push(TableStmtInfo {
                    stmt_type: "CREATE_VIEW".to_string(),
                    start_line: span.start.line as u32,
                    start_col: span.start.column as u32,
                    end_line: span.end.line as u32,
                    end_col: span.end.column as u32,
                    target: format!("{}.{}", target.db, target.table),
                    sources: srcs,
                    statement: extract_snippet(sql, span),
                });
                if !columns.is_empty() {
                    let cols = columns.iter().map(|c| ident_to_string(&c.name)).collect();
                    ctx.schemas.insert(target, cols);
                }
            }
            _ => {}
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnLineageInfo {
    pub stmt_index: usize,
    pub stmt_type: String,
    pub start_line: u32,
    pub start_col: u32,
    pub end_line: u32,
    pub end_col: u32,
    pub target: String,               // db.table
    pub column: String,               // target column name
    pub sources: Option<Vec<String>>, // db.table.col list if any
    pub expr: Option<String>,         // original expr when no sources
    pub statement: String,
}

/// Detailed per-statement column lineage with statement type and span positions.
pub fn analyze_sql_lineage_detailed(sql: &str) -> Result<Vec<ColumnLineageInfo>> {
    let dialect = HiveDialect {};
    let mut ctx = Context::default();
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| anyhow!(e.to_string()))?;
    let mut out: Vec<ColumnLineageInfo> = Vec::new();

    for (idx, stmt) in statements.into_iter().enumerate() {
        let span = stmt.span();
        match stmt {
            Statement::Use(u) => {
                if let Some(db) = use_to_db(&u)? {
                    ctx.current_db = Some(db);
                }
            }
            Statement::Set(_) => {}
            Statement::CreateTable(ct) => {
                let name = &ct.name;
                let columns = &ct.columns;
                let query = &ct.query;
                if !columns.is_empty() {
                    let (db, table) = qualify_table(&ctx, name)?;
                    let key = TableKey { db, table };
                    let cols = columns.iter().map(|c| ident_to_string(&c.name)).collect();
                    ctx.schemas.insert(key, cols);
                }
                if let Some(q) = query.as_ref() {
                    let (db, table) = qualify_table(&ctx, name)?;
                    let target_table = TableKey { db, table };

                    let select = expect_simple_select(q)?;
                    let cte_defs = build_cte_defs(&ctx, q)?;
                    let (alias_map, cte_aliases, derived_aliases) =
                        build_alias_map_with_cte(&ctx, &select.from, &cte_defs)?;
                    let default_only_table = only_table_in_from(&alias_map);
                    let default_only_cte = only_cte_in_from(&cte_aliases, &alias_map);
                    let default_only_derived =
                        only_derived_in_from(&derived_aliases, &alias_map, &cte_aliases);

                    let expanded = expand_select_items(
                        select,
                        &ctx,
                        &alias_map,
                        &cte_aliases,
                        &derived_aliases,
                        &cte_defs,
                    )?;
                    let target_cols: Vec<String> =
                        expanded.iter().map(|e| e.out_name.clone()).collect();
                    let named_windows = build_named_window_map(select);
                    for (i, exp) in expanded.iter().enumerate() {
                        let (mut sources, mut has_any_col) =
                            collect_sources_from_select_item_with_cte(
                                &exp.item,
                                default_only_table.as_ref(),
                                default_only_cte.as_ref(),
                                default_only_derived.as_ref(),
                                &alias_map,
                                &cte_aliases,
                                &derived_aliases,
                                &cte_defs,
                                Some(&named_windows),
                            )?;
                        union_group_having(
                            select,
                            default_only_table.as_ref(),
                            default_only_cte.as_ref(),
                            default_only_derived.as_ref(),
                            &alias_map,
                            &cte_aliases,
                            &derived_aliases,
                            &cte_defs,
                            Some(&named_windows),
                            &mut sources,
                        );
                        if !exp.is_from_wildcard && sources.is_empty() {
                            if let Some(single_tbl) = default_only_table.as_ref() {
                                let expr_sql = select_item_expr_sql_with_cte_and_derived(
                                    &exp.item,
                                    &cte_aliases,
                                    &derived_aliases,
                                    &cte_defs,
                                )?;
                                sources.insert(ColumnRef {
                                    table: (*single_tbl).clone(),
                                    column: expr_sql,
                                });
                                has_any_col = true;
                            } else {
                                let mut allow_fallback =
                                    matches!(exp.item, SelectItem::UnnamedExpr(_));
                                if let SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) =
                                    &exp.item
                                {
                                    if idents.len() == 2 {
                                        let qual = ident_to_string(&idents[0]);
                                        let col = ident_to_string(&idents[1]);
                                        if let Some(info) = derived_aliases.get(&qual) {
                                            if info.exprs.contains_key(&col) {
                                                allow_fallback = false;
                                            }
                                        }
                                        if let Some(cte_name) = cte_aliases.get(&qual) {
                                            if let Some(info) = cte_defs.defs.get(cte_name) {
                                                if info.exprs.contains_key(&col) {
                                                    allow_fallback = false;
                                                }
                                            }
                                        }
                                    }
                                }
                                if allow_fallback {
                                    let base_tables = collect_base_tables_in_scope(
                                        &alias_map,
                                        &cte_aliases,
                                        &derived_aliases,
                                        &cte_defs,
                                    );
                                    if !base_tables.is_empty() {
                                        sources = base_tables
                                            .into_iter()
                                            .map(|t| ColumnRef {
                                                table: t,
                                                column: "*".to_string(),
                                            })
                                            .collect();
                                        has_any_col = true;
                                    }
                                }
                            }
                        }
                        if has_any_col {
                            let mut srcs: Vec<String> =
                                sources.iter().map(|c| c.to_string()).collect();
                            srcs.sort();
                            out.push(ColumnLineageInfo {
                                stmt_index: idx,
                                stmt_type: "CTAS".to_string(),
                                start_line: span.start.line as u32,
                                start_col: span.start.column as u32,
                                end_line: span.end.line as u32,
                                end_col: span.end.column as u32,
                                target: format!("{}.{}", target_table.db, target_table.table),
                                column: target_cols[i].clone(),
                                sources: Some(srcs),
                                expr: None,
                                statement: extract_snippet(sql, span),
                            });
                        } else {
                            let expr_sql = select_item_expr_sql_with_cte_and_derived(
                                &exp.item,
                                &cte_aliases,
                                &derived_aliases,
                                &cte_defs,
                            )?;
                            out.push(ColumnLineageInfo {
                                stmt_index: idx,
                                stmt_type: "CTAS".to_string(),
                                start_line: span.start.line as u32,
                                start_col: span.start.column as u32,
                                end_line: span.end.line as u32,
                                end_col: span.end.column as u32,
                                target: format!("{}.{}", target_table.db, target_table.table),
                                column: target_cols[i].clone(),
                                sources: None,
                                expr: Some(expr_sql),
                                statement: extract_snippet(sql, span),
                            });
                        }
                    }
                }
            }
            Statement::Insert(ins) => {
                let (db, table) = match &ins.table {
                    TableObject::TableName(name) => qualify_table(&ctx, name)?,
                    _ => continue,
                };
                let target_table = TableKey { db, table };
                if let Some(source_q) = ins.source.as_ref() {
                    let select = expect_simple_select(source_q)?;
                    let cte_defs = build_cte_defs(&ctx, source_q)?;
                    let (alias_map, cte_aliases, derived_aliases) =
                        build_alias_map_with_cte(&ctx, &select.from, &cte_defs)?;
                    let default_only_table = only_table_in_from(&alias_map);
                    let default_only_cte = only_cte_in_from(&cte_aliases, &alias_map);
                    let default_only_derived =
                        only_derived_in_from(&derived_aliases, &alias_map, &cte_aliases);
                    let expanded = expand_select_items(
                        select,
                        &ctx,
                        &alias_map,
                        &cte_aliases,
                        &derived_aliases,
                        &cte_defs,
                    )?;
                    // target columns list similar于 analyze_sql_lineage，优先显式列，否则 schema，否则用 expanded 名
                    let target_cols: Vec<String> = if !ins.columns.is_empty() {
                        ins.columns.iter().map(ident_to_string).collect()
                    } else if let Some(cols) = ctx.schemas.get(&target_table) {
                        cols.clone()
                    } else {
                        expanded.iter().map(|e| e.out_name.clone()).collect()
                    };
                    let named_windows = build_named_window_map(select);
                    for (i, exp) in expanded.iter().enumerate() {
                        let (mut sources, mut has_any_col) =
                            collect_sources_from_select_item_with_cte(
                                &exp.item,
                                default_only_table.as_ref(),
                                default_only_cte.as_ref(),
                                default_only_derived.as_ref(),
                                &alias_map,
                                &cte_aliases,
                                &derived_aliases,
                                &cte_defs,
                                Some(&named_windows),
                            )?;
                        union_group_having(
                            select,
                            default_only_table.as_ref(),
                            default_only_cte.as_ref(),
                            default_only_derived.as_ref(),
                            &alias_map,
                            &cte_aliases,
                            &derived_aliases,
                            &cte_defs,
                            Some(&named_windows),
                            &mut sources,
                        );
                        if !exp.is_from_wildcard && sources.is_empty() {
                            if let Some(single_tbl) = default_only_table.as_ref() {
                                let expr_sql = select_item_expr_sql_with_cte_and_derived(
                                    &exp.item,
                                    &cte_aliases,
                                    &derived_aliases,
                                    &cte_defs,
                                )?;
                                sources.insert(ColumnRef {
                                    table: (*single_tbl).clone(),
                                    column: expr_sql,
                                });
                                has_any_col = true;
                            } else {
                                let mut allow_fallback =
                                    matches!(exp.item, SelectItem::UnnamedExpr(_));
                                if let SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) =
                                    &exp.item
                                {
                                    if idents.len() == 2 {
                                        let qual = ident_to_string(&idents[0]);
                                        let col = ident_to_string(&idents[1]);
                                        if let Some(info) = derived_aliases.get(&qual) {
                                            if info.exprs.contains_key(&col) {
                                                allow_fallback = false;
                                            }
                                        }
                                        if let Some(cte_name) = cte_aliases.get(&qual) {
                                            if let Some(info) = cte_defs.defs.get(cte_name) {
                                                if info.exprs.contains_key(&col) {
                                                    allow_fallback = false;
                                                }
                                            }
                                        }
                                    }
                                }
                                if allow_fallback {
                                    let base_tables = collect_base_tables_in_scope(
                                        &alias_map,
                                        &cte_aliases,
                                        &derived_aliases,
                                        &cte_defs,
                                    );
                                    if !base_tables.is_empty() {
                                        sources = base_tables
                                            .into_iter()
                                            .map(|t| ColumnRef {
                                                table: t,
                                                column: "*".to_string(),
                                            })
                                            .collect();
                                        has_any_col = true;
                                    }
                                }
                            }
                        }
                        let col_name = target_cols
                            .get(i)
                            .cloned()
                            .unwrap_or_else(|| exp.out_name.clone());
                        if has_any_col {
                            let mut srcs: Vec<String> =
                                sources.iter().map(|c| c.to_string()).collect();
                            srcs.sort();
                            out.push(ColumnLineageInfo {
                                stmt_index: idx,
                                stmt_type: "INSERT".to_string(),
                                start_line: span.start.line as u32,
                                start_col: span.start.column as u32,
                                end_line: span.end.line as u32,
                                end_col: span.end.column as u32,
                                target: format!("{}.{}", target_table.db, target_table.table),
                                column: col_name,
                                sources: Some(srcs),
                                expr: None,
                                statement: extract_snippet(sql, span),
                            });
                        } else {
                            let expr_sql = select_item_expr_sql_with_cte_and_derived(
                                &exp.item,
                                &cte_aliases,
                                &derived_aliases,
                                &cte_defs,
                            )?;
                            out.push(ColumnLineageInfo {
                                stmt_index: idx,
                                stmt_type: "INSERT".to_string(),
                                start_line: span.start.line as u32,
                                start_col: span.start.column as u32,
                                end_line: span.end.line as u32,
                                end_col: span.end.column as u32,
                                target: format!("{}.{}", target_table.db, target_table.table),
                                column: col_name,
                                sources: None,
                                expr: Some(expr_sql),
                                statement: extract_snippet(sql, span),
                            });
                        }
                    }
                }
            }
            Statement::CreateView {
                name,
                query,
                columns,
                ..
            } => {
                let (db, table) = qualify_table(&ctx, &name)?;
                let target_table = TableKey { db, table };
                let select = expect_simple_select(&query)?;
                let cte_defs = build_cte_defs(&ctx, &query)?;
                let (alias_map, cte_aliases, derived_aliases) =
                    build_alias_map_with_cte(&ctx, &select.from, &cte_defs)?;
                let default_only_table = only_table_in_from(&alias_map);
                let default_only_cte = only_cte_in_from(&cte_aliases, &alias_map);
                let default_only_derived =
                    only_derived_in_from(&derived_aliases, &alias_map, &cte_aliases);
                let expanded = expand_select_items(
                    select,
                    &ctx,
                    &alias_map,
                    &cte_aliases,
                    &derived_aliases,
                    &cte_defs,
                )?;
                let target_cols: Vec<String> = if !columns.is_empty() {
                    columns.iter().map(|c| ident_to_string(&c.name)).collect()
                } else {
                    expanded.iter().map(|e| e.out_name.clone()).collect()
                };
                let named_windows = build_named_window_map(select);
                for (i, exp) in expanded.iter().enumerate() {
                    let (mut sources, mut has_any_col) = collect_sources_from_select_item_with_cte(
                        &exp.item,
                        default_only_table.as_ref(),
                        default_only_cte.as_ref(),
                        default_only_derived.as_ref(),
                        &alias_map,
                        &cte_aliases,
                        &derived_aliases,
                        &cte_defs,
                        Some(&named_windows),
                    )?;
                    union_group_having(
                        select,
                        default_only_table.as_ref(),
                        default_only_cte.as_ref(),
                        default_only_derived.as_ref(),
                        &alias_map,
                        &cte_aliases,
                        &derived_aliases,
                        &cte_defs,
                        Some(&named_windows),
                        &mut sources,
                    );
                    if !exp.is_from_wildcard && sources.is_empty() {
                        let mut allow_fallback = matches!(exp.item, SelectItem::UnnamedExpr(_));
                        if let SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) = &exp.item
                        {
                            if idents.len() == 2 {
                                let qual = ident_to_string(&idents[0]);
                                let col = ident_to_string(&idents[1]);
                                if let Some(info) = derived_aliases.get(&qual) {
                                    if info.exprs.contains_key(&col) {
                                        allow_fallback = false;
                                    }
                                }
                                if let Some(cte_name) = cte_aliases.get(&qual) {
                                    if let Some(info) = cte_defs.defs.get(cte_name) {
                                        if info.exprs.contains_key(&col) {
                                            allow_fallback = false;
                                        }
                                    }
                                }
                            }
                        }
                        if allow_fallback {
                            let base_tables = collect_base_tables_in_scope(
                                &alias_map,
                                &cte_aliases,
                                &derived_aliases,
                                &cte_defs,
                            );
                            if !base_tables.is_empty() {
                                sources = base_tables
                                    .into_iter()
                                    .map(|t| ColumnRef {
                                        table: t,
                                        column: "*".to_string(),
                                    })
                                    .collect();
                                has_any_col = true;
                            }
                        }
                    }
                    let col_name = target_cols
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| exp.out_name.clone());
                    if has_any_col {
                        let mut srcs: Vec<String> = sources.iter().map(|c| c.to_string()).collect();
                        srcs.sort();
                        out.push(ColumnLineageInfo {
                            stmt_index: idx,
                            stmt_type: "CREATE_VIEW".to_string(),
                            start_line: span.start.line as u32,
                            start_col: span.start.column as u32,
                            end_line: span.end.line as u32,
                            end_col: span.end.column as u32,
                            target: format!("{}.{}", target_table.db, target_table.table),
                            column: col_name,
                            sources: Some(srcs),
                            expr: None,
                            statement: extract_snippet(sql, span),
                        });
                    } else {
                        let expr_sql = select_item_expr_sql_with_cte_and_derived(
                            &exp.item,
                            &cte_aliases,
                            &derived_aliases,
                            &cte_defs,
                        )?;
                        out.push(ColumnLineageInfo {
                            stmt_index: idx,
                            stmt_type: "CREATE_VIEW".to_string(),
                            start_line: span.start.line as u32,
                            start_col: span.start.column as u32,
                            end_line: span.end.line as u32,
                            end_col: span.end.column as u32,
                            target: format!("{}.{}", target_table.db, target_table.table),
                            column: col_name,
                            sources: None,
                            expr: Some(expr_sql),
                            statement: extract_snippet(sql, span),
                        });
                    }
                }
            }
            _ => {}
        }
    }
    Ok(out)
}

fn only_table_in_from(alias_map: &HashMap<String, TableKey>) -> Option<TableKey> {
    // If only one real table was referenced, return it to disambiguate bare column names.
    let unique_tables: BTreeSet<_> = alias_map.values().collect();
    if unique_tables.len() == 1 {
        unique_tables.iter().next().cloned().cloned()
    } else {
        None
    }
}

fn qualify_table(ctx: &Context, name: &ObjectName) -> Result<(String, String)> {
    let parts: Vec<String> = object_name_parts(name)?;
    match parts.len() {
        1 => {
            let db = ctx.current_db.clone().ok_or_else(|| {
                anyhow!(
                    "Unqualified table name '{}' without active database (USE) context",
                    parts[0]
                )
            })?;
            Ok((db, parts[0].clone()))
        }
        2 => Ok((parts[0].clone(), parts[1].clone())),
        _ => bail!("Unsupported multipart object name: {}", name),
    }
}

fn ident_to_string(ident: &Ident) -> String {
    ident.value.clone()
}

fn object_name_parts(name: &ObjectName) -> Result<Vec<String>> {
    let mut parts: Vec<String> = Vec::with_capacity(name.0.len());
    for part in &name.0 {
        match part {
            ObjectNamePart::Identifier(ident) => parts.push(ident_to_string(ident)),
            ObjectNamePart::Function(_) => bail!("Unsupported function in object name: {}", name),
        }
    }
    Ok(parts)
}

fn expect_simple_select(query: &Query) -> Result<&Select> {
    match query.body.as_ref() {
        SetExpr::Select(select) => Ok(select),
        other => bail!(
            "Only simple SELECT is supported in this analyzer, got {}",
            other
        ),
    }
}

fn list_sources_in_order(from: &[TableWithJoins], cte_defs: &CteDefs) -> Vec<(String, SourceKind)> {
    // Return a vec of (qualifier, SourceKind) in FROM order
    let mut v = Vec::new();
    for twj in from {
        collect_source_entry(&twj.relation, cte_defs, &mut v);
        for j in &twj.joins {
            collect_source_entry(&j.relation, cte_defs, &mut v);
        }
    }
    v
}

#[derive(Debug, Clone)]
enum SourceKind {
    Physical(()),
    Cte(String),
    Derived,
}

fn collect_source_entry(
    factor: &TableFactor,
    cte_defs: &CteDefs,
    out: &mut Vec<(String, SourceKind)>,
) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| p.as_ident().map(ident_to_string))
                .collect();
            let qual = if let Some(TableAlias {
                name: alias_ident, ..
            }) = alias
            {
                ident_to_string(alias_ident)
            } else {
                parts.last().cloned().unwrap_or_else(|| name.to_string())
            };
            if parts.len() == 1 && cte_defs.defs.contains_key(&parts[0]) {
                out.push((qual, SourceKind::Cte(parts[0].clone())));
            } else {
                // physical; we don't need inner payload here
                out.push((qual, SourceKind::Physical(())));
            }
        }
        TableFactor::Derived {
            alias: Some(alias), ..
        } => {
            out.push((ident_to_string(&alias.name), SourceKind::Derived));
        }
        _ => {}
    }
}

fn expand_select_items(
    select: &Select,
    ctx: &Context,
    alias_map: &HashMap<String, TableKey>,
    cte_aliases: &HashMap<String, String>,
    derived_aliases: &HashMap<String, DerivedInfo>,
    cte_defs: &CteDefs,
) -> Result<Vec<ExpandedItem>> {
    let mut out: Vec<ExpandedItem> = Vec::new();
    let sources_in_order = list_sources_in_order(&select.from, cte_defs);

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (qual, kind) in &sources_in_order {
                    match kind {
                        SourceKind::Physical(_) => {
                            if let Some(tk) = alias_map.get(qual) {
                                if let Some(cols) = ctx.schemas.get(tk) {
                                    for c in cols {
                                        let expr = Expr::CompoundIdentifier(vec![
                                            Ident::new(qual.clone()),
                                            Ident::new(c.clone()),
                                        ]);
                                        out.push(ExpandedItem {
                                            item: SelectItem::UnnamedExpr(expr),
                                            out_name: c.clone(),
                                            is_from_wildcard: true,
                                        });
                                    }
                                } else {
                                    bail!(
                                        "Unknown schema for table {}.{} to expand *",
                                        tk.db,
                                        tk.table
                                    );
                                }
                            }
                        }
                        SourceKind::Cte(cte_name) => {
                            if let Some(info) = cte_defs.defs.get(cte_name) {
                                for c in &info.columns {
                                    let expr = Expr::CompoundIdentifier(vec![
                                        Ident::new(qual.clone()),
                                        Ident::new(c.clone()),
                                    ]);
                                    out.push(ExpandedItem {
                                        item: SelectItem::UnnamedExpr(expr),
                                        out_name: c.clone(),
                                        is_from_wildcard: true,
                                    });
                                }
                            }
                        }
                        SourceKind::Derived => {
                            if let Some(info) = derived_aliases.get(qual) {
                                for c in &info.columns {
                                    let expr = Expr::CompoundIdentifier(vec![
                                        Ident::new(qual.clone()),
                                        Ident::new(c.clone()),
                                    ]);
                                    out.push(ExpandedItem {
                                        item: SelectItem::UnnamedExpr(expr),
                                        out_name: c.clone(),
                                        is_from_wildcard: true,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                let qual = match kind {
                    ast::SelectItemQualifiedWildcardKind::ObjectName(obj) => obj.to_string(),
                    ast::SelectItemQualifiedWildcardKind::Expr(_) => {
                        bail!("Unsupported expr.* qualified wildcard")
                    }
                };
                // Match qual against alias_map, cte_aliases, derived_aliases
                if let Some(tk) = alias_map.get(&qual) {
                    if let Some(cols) = ctx.schemas.get(tk) {
                        for c in cols {
                            let expr = Expr::CompoundIdentifier(vec![
                                Ident::new(qual.clone()),
                                Ident::new(c.clone()),
                            ]);
                            out.push(ExpandedItem {
                                item: SelectItem::UnnamedExpr(expr),
                                out_name: c.clone(),
                                is_from_wildcard: true,
                            });
                        }
                    } else {
                        bail!(
                            "Unknown schema for table {}.{} to expand {}.*",
                            tk.db,
                            tk.table,
                            qual
                        );
                    }
                } else if let Some(cte_name) = cte_aliases.get(&qual) {
                    if let Some(info) = cte_defs.defs.get(cte_name) {
                        for c in &info.columns {
                            let expr = Expr::CompoundIdentifier(vec![
                                Ident::new(qual.clone()),
                                Ident::new(c.clone()),
                            ]);
                            out.push(ExpandedItem {
                                item: SelectItem::UnnamedExpr(expr),
                                out_name: c.clone(),
                                is_from_wildcard: true,
                            });
                        }
                    }
                } else if let Some(info) = derived_aliases.get(&qual) {
                    for c in &info.columns {
                        let expr = Expr::CompoundIdentifier(vec![
                            Ident::new(qual.clone()),
                            Ident::new(c.clone()),
                        ]);
                        out.push(ExpandedItem {
                            item: SelectItem::UnnamedExpr(expr),
                            out_name: c.clone(),
                            is_from_wildcard: true,
                        });
                    }
                } else {
                    bail!("Unknown qualifier '{}' for wildcard expansion", qual);
                }
            }
            SelectItem::ExprWithAlias { alias, expr: _ } => {
                out.push(ExpandedItem {
                    item: item.clone(),
                    out_name: ident_to_string(alias),
                    is_from_wildcard: false,
                });
            }
            SelectItem::UnnamedExpr(expr) => {
                let out_name = match expr {
                    Expr::Identifier(ident) => ident_to_string(ident),
                    Expr::CompoundIdentifier(ids) => ids
                        .last()
                        .map(ident_to_string)
                        .unwrap_or_else(|| expr.to_string()),
                    _ => expr.to_string(),
                };
                out.push(ExpandedItem {
                    item: item.clone(),
                    out_name,
                    is_from_wildcard: false,
                });
            }
        }
    }

    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn union_group_having(
    select: &Select,
    default_only_table: Option<&TableKey>,
    default_only_cte: Option<&String>,
    default_only_derived: Option<&String>,
    alias_map: &HashMap<String, TableKey>,
    cte_aliases: &HashMap<String, String>,
    derived_aliases: &HashMap<String, DerivedInfo>,
    cte_defs: &CteDefs,
    named_windows: Option<&HashMap<String, ast::WindowSpec>>,
    acc: &mut BTreeSet<ColumnRef>,
) {
    // GROUP BY
    match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, _) => {
            for e in exprs {
                collect_columns_from_expr_with_cte(
                    e,
                    default_only_table,
                    default_only_cte,
                    default_only_derived,
                    alias_map,
                    cte_aliases,
                    derived_aliases,
                    cte_defs,
                    acc,
                    named_windows,
                );
            }
        }
        ast::GroupByExpr::All(_) => {}
    }
    // HAVING
    if let Some(having) = &select.having {
        collect_columns_from_expr_with_cte(
            having,
            default_only_table,
            default_only_cte,
            default_only_derived,
            alias_map,
            cte_aliases,
            derived_aliases,
            cte_defs,
            acc,
            named_windows,
        );
    }
}

fn build_named_window_map(select: &Select) -> HashMap<String, ast::WindowSpec> {
    use ast::{NamedWindowDefinition, NamedWindowExpr};
    let mut map_expr: HashMap<String, NamedWindowExpr> = HashMap::new();
    for NamedWindowDefinition(name, expr) in &select.named_window {
        map_expr.insert(name.value.clone(), expr.clone());
    }
    // Resolve to WindowSpec
    let mut map_spec: HashMap<String, ast::WindowSpec> = HashMap::new();
    for key in map_expr.keys().cloned().collect::<Vec<_>>() {
        if let Some(spec) = resolve_named_window_spec(&key, &map_expr, &mut HashMap::new()) {
            map_spec.insert(key, spec);
        }
    }
    map_spec
}

fn resolve_named_window_spec(
    name: &str,
    defs: &HashMap<String, ast::NamedWindowExpr>,
    visiting: &mut HashMap<String, bool>,
) -> Option<ast::WindowSpec> {
    if visiting.get(name).copied().unwrap_or(false) {
        return None;
    }
    visiting.insert(name.to_string(), true);
    match defs.get(name)? {
        ast::NamedWindowExpr::WindowSpec(spec) => Some(spec.clone()),
        ast::NamedWindowExpr::NamedWindow(id) => {
            resolve_named_window_spec(&id.value, defs, visiting)
        }
    }
}

fn collect_base_tables_in_scope(
    alias_map: &HashMap<String, TableKey>,
    cte_aliases: &HashMap<String, String>,
    derived_aliases: &HashMap<String, DerivedInfo>,
    cte_defs: &CteDefs,
) -> BTreeSet<TableKey> {
    let mut set: BTreeSet<TableKey> = BTreeSet::new();
    for tk in alias_map.values() {
        set.insert(tk.clone());
    }
    for cte_name in cte_aliases.values() {
        if let Some(info) = cte_defs.defs.get(cte_name) {
            for srcs in info.sources.values() {
                for s in srcs {
                    set.insert(s.table.clone());
                }
            }
        }
    }
    for info in derived_aliases.values() {
        for srcs in info.sources.values() {
            for s in srcs {
                set.insert(s.table.clone());
            }
        }
    }
    set
}

// Compute complete output mapping for a SELECT by first resolving all relations in FROM/JOIN
// (including nested derived subqueries and CTE chains), then collecting column-level sources.
fn analyze_select_info(ctx: &Context, select: &Select, cte_defs: &CteDefs) -> Result<DerivedInfo> {
    let (alias_map, cte_aliases, derived_aliases) =
        build_alias_map_with_cte(ctx, &select.from, cte_defs)?;
    let default_only_table = only_table_in_from(&alias_map);
    let default_only_cte = only_cte_in_from(&cte_aliases, &alias_map);
    let default_only_derived = only_derived_in_from(&derived_aliases, &alias_map, &cte_aliases);

    let expanded = expand_select_items(
        select,
        ctx,
        &alias_map,
        &cte_aliases,
        &derived_aliases,
        cte_defs,
    )?;
    let cols: Vec<String> = expanded.iter().map(|e| e.out_name.clone()).collect();
    let mut info = DerivedInfo {
        columns: cols.clone(),
        sources: HashMap::new(),
        exprs: HashMap::new(),
        sources_by_index: vec![None; cols.len()],
        exprs_by_index: vec![None; cols.len()],
    };
    let named_windows = build_named_window_map(select);

    for (i, exp) in expanded.iter().enumerate() {
        let (mut sources, mut has_any_col) = collect_sources_from_select_item_with_cte(
            &exp.item,
            default_only_table.as_ref(),
            default_only_cte.as_ref(),
            default_only_derived.as_ref(),
            &alias_map,
            &cte_aliases,
            &derived_aliases,
            cte_defs,
            Some(&named_windows),
        )?;
        union_group_having(
            select,
            default_only_table.as_ref(),
            default_only_cte.as_ref(),
            default_only_derived.as_ref(),
            &alias_map,
            &cte_aliases,
            &derived_aliases,
            cte_defs,
            Some(&named_windows),
            &mut sources,
        );

        if !exp.is_from_wildcard && sources.is_empty() {
            if let Some(single_tbl) = default_only_table.as_ref() {
                let expr_sql = select_item_expr_sql_with_cte_and_derived(
                    &exp.item,
                    &cte_aliases,
                    &derived_aliases,
                    cte_defs,
                )?;
                sources.insert(ColumnRef {
                    table: (*single_tbl).clone(),
                    column: expr_sql,
                });
                has_any_col = true;
            } else {
                let mut allow_fallback = matches!(exp.item, SelectItem::UnnamedExpr(_));
                if let SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) = &exp.item {
                    if idents.len() == 2 {
                        let qual = ident_to_string(&idents[0]);
                        let col = ident_to_string(&idents[1]);
                        if let Some(info_d) = derived_aliases.get(&qual) {
                            if info_d.exprs.contains_key(&col) {
                                allow_fallback = false;
                            }
                        }
                        if let Some(cte_name) = cte_aliases.get(&qual) {
                            if let Some(info_c) = cte_defs.defs.get(cte_name) {
                                if info_c.exprs.contains_key(&col) {
                                    allow_fallback = false;
                                }
                            }
                        }
                    }
                }
                if allow_fallback {
                    let base_tables = collect_base_tables_in_scope(
                        &alias_map,
                        &cte_aliases,
                        &derived_aliases,
                        cte_defs,
                    );
                    if !base_tables.is_empty() {
                        sources = base_tables
                            .into_iter()
                            .map(|t| ColumnRef {
                                table: t,
                                column: "*".to_string(),
                            })
                            .collect();
                        has_any_col = true;
                    }
                }
            }
        }

        let col_name = info.columns[i].clone();
        if has_any_col {
            info.sources.insert(col_name, sources.clone());
            info.sources_by_index[i] = Some(sources);
            info.exprs_by_index[i] = None;
        } else {
            let expr_sql = select_item_expr_sql_with_cte_and_derived(
                &exp.item,
                &cte_aliases,
                &derived_aliases,
                cte_defs,
            )?;
            info.exprs.insert(col_name, expr_sql.clone());
            info.exprs_by_index[i] = Some(expr_sql);
            info.sources_by_index[i] = None;
        }
    }

    Ok(info)
}

// Build alias map for physical tables and record CTE aliases used in FROM/JOIN
#[allow(clippy::type_complexity)]
fn build_alias_map_with_cte(
    ctx: &Context,
    from: &[TableWithJoins],
    cte_defs: &CteDefs,
) -> Result<(
    HashMap<String, TableKey>,
    HashMap<String, String>,
    HashMap<String, DerivedInfo>,
)> {
    let mut map: HashMap<String, TableKey> = HashMap::new();
    let mut cte_aliases: HashMap<String, String> = HashMap::new();
    let mut derived_aliases: HashMap<String, DerivedInfo> = HashMap::new();

    for twj in from {
        collect_from_factor_with_cte(
            ctx,
            &mut map,
            &mut cte_aliases,
            &mut derived_aliases,
            &twj.relation,
            cte_defs,
        )?;
        for j in &twj.joins {
            collect_from_factor_with_cte(
                ctx,
                &mut map,
                &mut cte_aliases,
                &mut derived_aliases,
                &j.relation,
                cte_defs,
            )?;
        }
    }
    Ok((map, cte_aliases, derived_aliases))
}

fn collect_from_factor_with_cte(
    ctx: &Context,
    map: &mut HashMap<String, TableKey>,
    cte_aliases: &mut HashMap<String, String>,
    derived_aliases: &mut HashMap<String, DerivedInfo>,
    factor: &TableFactor,
    cte_defs: &CteDefs,
) -> Result<()> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let parts = object_name_parts(name)?;
            if parts.len() == 1 {
                let base = parts[0].clone();
                if cte_defs.defs.contains_key(&base) {
                    // reference to CTE
                    if let Some(TableAlias {
                        name: alias_ident, ..
                    }) = alias
                    {
                        cte_aliases.insert(ident_to_string(alias_ident), base.clone());
                    }
                    cte_aliases.insert(base.clone(), base);
                    return Ok(());
                }
            }
            // physical table
            let (db, table) = qualify_table(ctx, name)?;
            let table_key = TableKey {
                db: db.clone(),
                table: table.clone(),
            };
            if let Some(TableAlias {
                name: alias_ident, ..
            }) = alias
            {
                map.insert(ident_to_string(alias_ident), table_key.clone());
            }
            map.insert(table, table_key);
        }
        TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } => {
            let info = build_derived_info(ctx, subquery, cte_defs, Some(alias))?;
            derived_aliases.insert(ident_to_string(&alias.name), info);
        }
        _ => {}
    }
    Ok(())
}

// Build CTE definitions for a query (top-level WITH)
fn build_cte_defs(ctx: &Context, query: &Query) -> Result<CteDefs> {
    let mut defs = CteDefs::default();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let cte_name = ident_to_string(&cte.alias.name);
            let select = expect_simple_select(&cte.query)?;

            // column names
            let col_names: Vec<String> = if !cte.alias.columns.is_empty() {
                cte.alias
                    .columns
                    .iter()
                    .map(|c| ident_to_string(&c.name))
                    .collect()
            } else {
                select
                    .projection
                    .iter()
                    .map(select_item_output_name)
                    .collect::<Result<Vec<_>>>()?
            };

            // alias map for CTE's FROM including earlier CTEs
            let (alias_map, cte_aliases, derived_aliases) =
                build_alias_map_with_cte(ctx, &select.from, &defs)?;
            let default_only_table = only_table_in_from(&alias_map);
            let default_only_cte = only_cte_in_from(&cte_aliases, &alias_map);
            let default_only_derived =
                only_derived_in_from(&derived_aliases, &alias_map, &cte_aliases);

            let expanded = expand_select_items(
                select,
                ctx,
                &alias_map,
                &cte_aliases,
                &derived_aliases,
                &defs,
            )?;
            let mut info = CteInfo {
                columns: col_names.clone(),
                sources: HashMap::new(),
                exprs: HashMap::new(),
            };
            let named_windows = build_named_window_map(select);
            for (i, exp) in expanded.iter().enumerate() {
                let (mut srcs, mut has_any) = collect_sources_from_select_item_with_cte(
                    &exp.item,
                    default_only_table.as_ref(),
                    default_only_cte.as_ref(),
                    default_only_derived.as_ref(),
                    &alias_map,
                    &cte_aliases,
                    &derived_aliases,
                    &defs,
                    Some(&named_windows),
                )?;
                if !exp.is_from_wildcard && srcs.is_empty() {
                    if let Some(single_tbl) = default_only_table.as_ref() {
                        let expr_sql = select_item_expr_sql_with_cte_and_derived(
                            &exp.item,
                            &cte_aliases,
                            &derived_aliases,
                            &defs,
                        )?;
                        srcs.insert(ColumnRef {
                            table: (*single_tbl).clone(),
                            column: expr_sql,
                        });
                        has_any = true;
                    } else if matches!(exp.item, SelectItem::UnnamedExpr(_)) {
                        let base_tables = collect_base_tables_in_scope(
                            &alias_map,
                            &cte_aliases,
                            &derived_aliases,
                            &defs,
                        );
                        if !base_tables.is_empty() {
                            srcs = base_tables
                                .into_iter()
                                .map(|t| ColumnRef {
                                    table: t,
                                    column: "*".to_string(),
                                })
                                .collect();
                            has_any = true;
                        }
                    }
                }
                let name = col_names
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{}", i + 1));
                if has_any {
                    info.sources.insert(name, srcs);
                } else {
                    let expr_sql = select_item_expr_sql_with_cte_and_derived(
                        &exp.item,
                        &cte_aliases,
                        &derived_aliases,
                        &defs,
                    )?;
                    info.exprs.insert(name, expr_sql);
                }
            }
            defs.defs.insert(cte_name, info);
        }
    }
    Ok(defs)
}

fn only_cte_in_from(
    cte_aliases: &HashMap<String, String>,
    alias_map: &HashMap<String, TableKey>,
) -> Option<String> {
    if !alias_map.is_empty() {
        return None;
    }
    let unique: BTreeSet<_> = cte_aliases.values().collect();
    if unique.len() == 1 {
        unique.iter().next().cloned().cloned()
    } else {
        None
    }
}

fn only_derived_in_from(
    derived_aliases: &HashMap<String, DerivedInfo>,
    alias_map: &HashMap<String, TableKey>,
    cte_aliases: &HashMap<String, String>,
) -> Option<String> {
    if !alias_map.is_empty() || !cte_aliases.is_empty() {
        return None;
    }
    if derived_aliases.len() == 1 {
        Some(derived_aliases.keys().next().cloned().unwrap())
    } else {
        None
    }
}

fn extract_snippet(sql: &str, span: Span) -> String {
    let mut res = String::new();
    let start_line = span.start.line.max(1) as usize;
    let end_line = span.end.line.max(1) as usize;
    let start_col = span.start.column.max(1) as usize;
    let end_col = span.end.column.max(1) as usize;
    let lines: Vec<&str> = sql.lines().collect();
    if start_line == end_line {
        if let Some(line) = lines.get(start_line - 1) {
            let s = line
                .chars()
                .skip(start_col - 1)
                .take(end_col.saturating_sub(start_col))
                .collect::<String>();
            return s;
        }
    } else {
        if let Some(line) = lines.get(start_line - 1) {
            res.push_str(&line.chars().skip(start_col - 1).collect::<String>());
            res.push('\n');
        }
        for l in (start_line + 1)..(end_line) {
            if let Some(line) = lines.get(l - 1) {
                res.push_str(line);
                res.push('\n');
            }
        }
        if let Some(line) = lines.get(end_line - 1) {
            res.push_str(&line.chars().take(end_col - 1).collect::<String>());
        }
    }
    res
}

fn build_derived_info(
    ctx: &Context,
    subquery: &Query,
    cte_defs: &CteDefs,
    alias: Option<&TableAlias>,
) -> Result<DerivedInfo> {
    let select = expect_simple_select(subquery)?;
    let (alias_map, cte_aliases, derived_aliases) =
        build_alias_map_with_cte(ctx, &select.from, cte_defs)?;
    let default_only_table = only_table_in_from(&alias_map);
    let default_only_cte = only_cte_in_from(&cte_aliases, &alias_map);
    let default_only_derived = only_derived_in_from(&derived_aliases, &alias_map, &cte_aliases);

    let col_names: Vec<String> = if let Some(alias) = alias {
        if !alias.columns.is_empty() {
            alias
                .columns
                .iter()
                .map(|c| ident_to_string(&c.name))
                .collect()
        } else {
            select
                .projection
                .iter()
                .map(select_item_output_name)
                .collect::<Result<Vec<_>>>()?
        }
    } else {
        select
            .projection
            .iter()
            .map(select_item_output_name)
            .collect::<Result<Vec<_>>>()?
    };

    let expanded = expand_select_items(
        select,
        ctx,
        &alias_map,
        &cte_aliases,
        &derived_aliases,
        cte_defs,
    )?;
    let mut info = DerivedInfo {
        columns: col_names.clone(),
        sources: HashMap::new(),
        exprs: HashMap::new(),
        sources_by_index: vec![None; col_names.len()],
        exprs_by_index: vec![None; col_names.len()],
    };
    let named_windows = build_named_window_map(select);
    for (i, exp) in expanded.iter().enumerate() {
        let (mut srcs, mut has_any) = collect_sources_from_select_item_with_cte(
            &exp.item,
            default_only_table.as_ref(),
            default_only_cte.as_ref(),
            default_only_derived.as_ref(),
            &alias_map,
            &cte_aliases,
            &derived_aliases,
            cte_defs,
            Some(&named_windows),
        )?;
        union_group_having(
            select,
            default_only_table.as_ref(),
            default_only_cte.as_ref(),
            default_only_derived.as_ref(),
            &alias_map,
            &cte_aliases,
            &derived_aliases,
            cte_defs,
            Some(&named_windows),
            &mut srcs,
        );
        if !exp.is_from_wildcard && srcs.is_empty() {
            if let Some(single_tbl) = default_only_table.as_ref() {
                let expr_sql = select_item_expr_sql_with_cte_and_derived(
                    &exp.item,
                    &cte_aliases,
                    &derived_aliases,
                    cte_defs,
                )?;
                srcs.insert(ColumnRef {
                    table: (*single_tbl).clone(),
                    column: expr_sql,
                });
                has_any = true;
            }
        }
        let name = col_names
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("col_{}", i + 1));
        if has_any {
            info.sources_by_index[i] = Some(srcs.clone());
            info.exprs_by_index[i] = None;
            info.sources.insert(name, srcs);
        } else {
            let expr_sql = select_item_expr_sql_with_cte_and_derived(
                &exp.item,
                &cte_aliases,
                &derived_aliases,
                cte_defs,
            )?;
            info.exprs_by_index[i] = Some(expr_sql.clone());
            info.sources_by_index[i] = None;
            info.exprs.insert(name, expr_sql);
        }
    }
    Ok(info)
}

fn select_item_output_name(item: &SelectItem) -> Result<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Ok(ident_to_string(alias)),
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Ok(ident_to_string(ident)),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => idents
            .last()
            .map(ident_to_string)
            .ok_or_else(|| anyhow!("Invalid compound identifier")),
        SelectItem::UnnamedExpr(expr) => Ok(expr.to_string()),
        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {
            bail!("Wildcard in projection not supported for lineage output names")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_sources_from_select_item_with_cte(
    item: &SelectItem,
    default_only_table: Option<&TableKey>,
    default_only_cte: Option<&String>,
    default_only_derived: Option<&String>,
    alias_map: &HashMap<String, TableKey>,
    cte_aliases: &HashMap<String, String>,
    derived_aliases: &HashMap<String, DerivedInfo>,
    cte_defs: &CteDefs,
    named_windows: Option<&HashMap<String, ast::WindowSpec>>,
) -> Result<(BTreeSet<ColumnRef>, bool)> {
    let expr = match item {
        SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::UnnamedExpr(expr) => expr,
        other => bail!("Unsupported select item in lineage analysis: {}", other),
    };
    let mut set: BTreeSet<ColumnRef> = BTreeSet::new();
    collect_columns_from_expr_with_cte(
        expr,
        default_only_table,
        default_only_cte,
        default_only_derived,
        alias_map,
        cte_aliases,
        derived_aliases,
        cte_defs,
        &mut set,
        named_windows,
    );
    let has_any = !set.is_empty();
    Ok((set, has_any))
}

#[allow(clippy::too_many_arguments, clippy::collapsible_match)]
fn collect_columns_from_expr_with_cte(
    expr: &Expr,
    default_only_table: Option<&TableKey>,
    default_only_cte: Option<&String>,
    default_only_derived: Option<&String>,
    alias_map: &HashMap<String, TableKey>,
    cte_aliases: &HashMap<String, String>,
    derived_aliases: &HashMap<String, DerivedInfo>,
    cte_defs: &CteDefs,
    acc: &mut BTreeSet<ColumnRef>,
    named_windows: Option<&HashMap<String, ast::WindowSpec>>,
) {
    match expr {
        Expr::Identifier(ident) => {
            if let Some(tbl) = default_only_table {
                acc.insert(ColumnRef {
                    table: tbl.clone(),
                    column: ident_to_string(ident),
                });
            } else if let Some(cte_name) = default_only_cte {
                if let Some(info) = cte_defs.defs.get(cte_name) {
                    if let Some(srcs) = info.sources.get(&ident_to_string(ident)) {
                        for s in srcs {
                            acc.insert(s.clone());
                        }
                    }
                }
            } else if let Some(derived_name) = default_only_derived {
                if let Some(info) = derived_aliases.get(derived_name) {
                    if let Some(srcs) = info.sources.get(&ident_to_string(ident)) {
                        for s in srcs {
                            acc.insert(s.clone());
                        }
                    }
                }
            }
        }
        Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                let qualifier = ident_to_string(&idents[0]);
                let col = ident_to_string(&idents[1]);
                if let Some(tbl) = alias_map.get(&qualifier) {
                    acc.insert(ColumnRef {
                        table: tbl.clone(),
                        column: col,
                    });
                } else if let Some(cte_name) = cte_aliases.get(&qualifier) {
                    if let Some(info) = cte_defs.defs.get(cte_name) {
                        if let Some(srcs) = info.sources.get(&col) {
                            for s in srcs {
                                acc.insert(s.clone());
                            }
                        }
                    }
                } else if let Some(info) = derived_aliases.get(&qualifier) {
                    if let Some(srcs) = info.sources.get(&col) {
                        for s in srcs {
                            acc.insert(s.clone());
                        }
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_columns_from_expr_with_cte(
                left,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
            collect_columns_from_expr_with_cte(
                right,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
        }
        Expr::UnaryOp { expr, .. } => collect_columns_from_expr_with_cte(
            expr,
            default_only_table,
            default_only_cte,
            default_only_derived,
            alias_map,
            cte_aliases,
            derived_aliases,
            cte_defs,
            acc,
            named_windows,
        ),
        Expr::Nested(e) => collect_columns_from_expr_with_cte(
            e,
            default_only_table,
            default_only_cte,
            default_only_derived,
            alias_map,
            cte_aliases,
            derived_aliases,
            cte_defs,
            acc,
            named_windows,
        ),
        Expr::Function(fun) => {
            match &fun.args {
                ast::FunctionArguments::None => {}
                ast::FunctionArguments::Subquery(_) => {}
                ast::FunctionArguments::List(list) => {
                    for arg in &list.args {
                        match arg {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                collect_columns_from_expr_with_cte(
                                    e,
                                    default_only_table,
                                    default_only_cte,
                                    default_only_derived,
                                    alias_map,
                                    cte_aliases,
                                    derived_aliases,
                                    cte_defs,
                                    acc,
                                    named_windows,
                                )
                            }
                            ast::FunctionArg::Named { arg, .. }
                            | ast::FunctionArg::ExprNamed { arg, .. } => {
                                if let ast::FunctionArgExpr::Expr(e) = arg {
                                    collect_columns_from_expr_with_cte(
                                        e,
                                        default_only_table,
                                        default_only_cte,
                                        default_only_derived,
                                        alias_map,
                                        cte_aliases,
                                        derived_aliases,
                                        cte_defs,
                                        acc,
                                        named_windows,
                                    )
                                }
                            }
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::QualifiedWildcard(
                                _,
                            ))
                            | ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                                // ignore wildcards inside function args
                            }
                        }
                    }
                }
            }
            if let Some(f) = &fun.filter {
                collect_columns_from_expr_with_cte(
                    f,
                    default_only_table,
                    default_only_cte,
                    default_only_derived,
                    alias_map,
                    cte_aliases,
                    derived_aliases,
                    cte_defs,
                    acc,
                    named_windows,
                );
            }
            if let Some(over) = &fun.over {
                match over {
                    ast::WindowType::WindowSpec(spec) => {
                        for e in &spec.partition_by {
                            collect_columns_from_expr_with_cte(
                                e,
                                default_only_table,
                                default_only_cte,
                                default_only_derived,
                                alias_map,
                                cte_aliases,
                                derived_aliases,
                                cte_defs,
                                acc,
                                named_windows,
                            );
                        }
                        for ob in &spec.order_by {
                            collect_columns_from_expr_with_cte(
                                &ob.expr,
                                default_only_table,
                                default_only_cte,
                                default_only_derived,
                                alias_map,
                                cte_aliases,
                                derived_aliases,
                                cte_defs,
                                acc,
                                named_windows,
                            );
                        }
                        if let Some(frame) = &spec.window_frame {
                            match &frame.start_bound {
                                ast::WindowFrameBound::Preceding(Some(n))
                                | ast::WindowFrameBound::Following(Some(n)) => {
                                    collect_columns_from_expr_with_cte(
                                        n,
                                        default_only_table,
                                        default_only_cte,
                                        default_only_derived,
                                        alias_map,
                                        cte_aliases,
                                        derived_aliases,
                                        cte_defs,
                                        acc,
                                        named_windows,
                                    )
                                }
                                _ => {}
                            }
                            if let Some(endb) = &frame.end_bound {
                                match endb {
                                    ast::WindowFrameBound::Preceding(Some(n))
                                    | ast::WindowFrameBound::Following(Some(n)) => {
                                        collect_columns_from_expr_with_cte(
                                            n,
                                            default_only_table,
                                            default_only_cte,
                                            default_only_derived,
                                            alias_map,
                                            cte_aliases,
                                            derived_aliases,
                                            cte_defs,
                                            acc,
                                            named_windows,
                                        )
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    ast::WindowType::NamedWindow(id) => {
                        if let Some(map) = named_windows {
                            if let Some(spec) = map.get(&id.value) {
                                for e in &spec.partition_by {
                                    collect_columns_from_expr_with_cte(
                                        e,
                                        default_only_table,
                                        default_only_cte,
                                        default_only_derived,
                                        alias_map,
                                        cte_aliases,
                                        derived_aliases,
                                        cte_defs,
                                        acc,
                                        named_windows,
                                    );
                                }
                                for ob in &spec.order_by {
                                    collect_columns_from_expr_with_cte(
                                        &ob.expr,
                                        default_only_table,
                                        default_only_cte,
                                        default_only_derived,
                                        alias_map,
                                        cte_aliases,
                                        derived_aliases,
                                        cte_defs,
                                        acc,
                                        named_windows,
                                    );
                                }
                                if let Some(frame) = &spec.window_frame {
                                    match &frame.start_bound {
                                        ast::WindowFrameBound::Preceding(Some(n))
                                        | ast::WindowFrameBound::Following(Some(n)) => {
                                            collect_columns_from_expr_with_cte(
                                                n,
                                                default_only_table,
                                                default_only_cte,
                                                default_only_derived,
                                                alias_map,
                                                cte_aliases,
                                                derived_aliases,
                                                cte_defs,
                                                acc,
                                                named_windows,
                                            )
                                        }
                                        _ => {}
                                    }
                                    if let Some(endb) = &frame.end_bound {
                                        match endb {
                                            ast::WindowFrameBound::Preceding(Some(n))
                                            | ast::WindowFrameBound::Following(Some(n)) => {
                                                collect_columns_from_expr_with_cte(
                                                    n,
                                                    default_only_table,
                                                    default_only_cte,
                                                    default_only_derived,
                                                    alias_map,
                                                    cte_aliases,
                                                    derived_aliases,
                                                    cte_defs,
                                                    acc,
                                                    named_windows,
                                                )
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for ob in &fun.within_group {
                collect_columns_from_expr_with_cte(
                    &ob.expr,
                    default_only_table,
                    default_only_cte,
                    default_only_derived,
                    alias_map,
                    cte_aliases,
                    derived_aliases,
                    cte_defs,
                    acc,
                    named_windows,
                );
            }
        }
        Expr::Cast { expr, .. } => collect_columns_from_expr_with_cte(
            expr,
            default_only_table,
            default_only_cte,
            default_only_derived,
            alias_map,
            cte_aliases,
            derived_aliases,
            cte_defs,
            acc,
            named_windows,
        ),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_columns_from_expr_with_cte(
                    op,
                    default_only_table,
                    default_only_cte,
                    default_only_derived,
                    alias_map,
                    cte_aliases,
                    derived_aliases,
                    cte_defs,
                    acc,
                    named_windows,
                );
            }
            for c in conditions {
                collect_columns_from_expr_with_cte(
                    &c.condition,
                    default_only_table,
                    default_only_cte,
                    default_only_derived,
                    alias_map,
                    cte_aliases,
                    derived_aliases,
                    cte_defs,
                    acc,
                    named_windows,
                );
                collect_columns_from_expr_with_cte(
                    &c.result,
                    default_only_table,
                    default_only_cte,
                    default_only_derived,
                    alias_map,
                    cte_aliases,
                    derived_aliases,
                    cte_defs,
                    acc,
                    named_windows,
                );
            }
            if let Some(e) = else_result {
                collect_columns_from_expr_with_cte(
                    e,
                    default_only_table,
                    default_only_cte,
                    default_only_derived,
                    alias_map,
                    cte_aliases,
                    derived_aliases,
                    cte_defs,
                    acc,
                    named_windows,
                );
            }
        }
        Expr::Like { expr, pattern, .. } => {
            collect_columns_from_expr_with_cte(
                expr,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
            collect_columns_from_expr_with_cte(
                pattern,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
        }
        Expr::ILike { expr, pattern, .. } => {
            collect_columns_from_expr_with_cte(
                expr,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
            collect_columns_from_expr_with_cte(
                pattern,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_columns_from_expr_with_cte(
                expr,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
            collect_columns_from_expr_with_cte(
                low,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
            collect_columns_from_expr_with_cte(
                high,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
        }
        Expr::InList { expr, list, .. } => {
            collect_columns_from_expr_with_cte(
                expr,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
            for e in list {
                collect_columns_from_expr_with_cte(
                    e,
                    default_only_table,
                    default_only_cte,
                    default_only_derived,
                    alias_map,
                    cte_aliases,
                    derived_aliases,
                    cte_defs,
                    acc,
                    named_windows,
                );
            }
        }
        Expr::InSubquery { expr, .. } => {
            collect_columns_from_expr_with_cte(
                expr,
                default_only_table,
                default_only_cte,
                default_only_derived,
                alias_map,
                cte_aliases,
                derived_aliases,
                cte_defs,
                acc,
                named_windows,
            );
        }
        Expr::GroupingSets(sets) => {
            for group in sets {
                for e in group {
                    collect_columns_from_expr_with_cte(
                        e,
                        default_only_table,
                        default_only_cte,
                        default_only_derived,
                        alias_map,
                        cte_aliases,
                        derived_aliases,
                        cte_defs,
                        acc,
                        named_windows,
                    );
                }
            }
        }
        Expr::Cube(sets) | Expr::Rollup(sets) => {
            for group in sets {
                for e in group {
                    collect_columns_from_expr_with_cte(
                        e,
                        default_only_table,
                        default_only_cte,
                        default_only_derived,
                        alias_map,
                        cte_aliases,
                        derived_aliases,
                        cte_defs,
                        acc,
                        named_windows,
                    );
                }
            }
        }
        _ => {}
    }
}

fn select_item_expr_sql_with_cte_and_derived(
    item: &SelectItem,
    cte_aliases: &HashMap<String, String>,
    derived_aliases: &HashMap<String, DerivedInfo>,
    cte_defs: &CteDefs,
) -> Result<String> {
    match item {
        SelectItem::ExprWithAlias { expr, .. } | SelectItem::UnnamedExpr(expr) => match expr {
            Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                let qualifier = ident_to_string(&idents[0]);
                let col = ident_to_string(&idents[1]);
                if let Some(cte_name) = cte_aliases.get(&qualifier) {
                    if let Some(info) = cte_defs.defs.get(cte_name) {
                        if let Some(e) = info.exprs.get(&col) {
                            return Ok(e.clone());
                        }
                    }
                }
                if let Some(info) = derived_aliases.get(&qualifier) {
                    if let Some(e) = info.exprs.get(&col) {
                        return Ok(e.clone());
                    }
                }
                Ok(expr.to_string())
            }
            _ => Ok(expr.to_string()),
        },
        other => Ok(other.to_string()),
    }
}

fn use_to_db(u: &Use) -> Result<Option<String>> {
    let obj_name_opt = match u {
        Use::Catalog(name)
        | Use::Schema(name)
        | Use::Database(name)
        | Use::Warehouse(name)
        | Use::Role(name)
        | Use::Object(name) => Some(name),
        Use::SecondaryRoles(_) | Use::Default => None,
    };
    if let Some(name) = obj_name_opt {
        let parts = object_name_parts(name)?;
        if parts.is_empty() {
            Ok(None)
        } else {
            // Use the last part as database/schema name
            Ok(Some(parts.last().unwrap().clone()))
        }
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn sorted(lines: Vec<String>) -> Vec<String> {
        let mut v = lines;
        v.sort();
        v
    }

    #[test]
    fn test_ctas_and_inserts_with_use() -> Result<()> {
        let sql = r#"
            USE db1;
            CREATE TABLE source_a (
                id INT,
                name STRING,
                v1 INT,
                v2 INT
            );

            CREATE TABLE source_b (
                id INT,
                name STRING,
                amount DECIMAL(10,2)
            );

            -- CTAS with expressions and aliases
            CREATE TABLE target1 AS
            SELECT
                a.id as id,
                upper(a.name) as name_upper,
                a.v1 + a.v2 as sum_v,
                1 as one,
                current_timestamp() as ts
            FROM source_a a;

            -- INSERT INTO with explicit target columns and join
            CREATE TABLE target2 (id INT, user_name STRING, total_amount DECIMAL(10,2));
            INSERT INTO target2 (id, user_name, total_amount)
            SELECT a.id, b.name, b.amount * 2 FROM source_a a JOIN source_b b ON a.id = b.id;

            -- INSERT OVERWRITE with qualified target and unqualified single-source columns
            CREATE TABLE db2.target3 (id INT, newname STRING);
            INSERT OVERWRITE TABLE db2.target3
            SELECT id, concat(name, 'x') as newname FROM source_a;
        "#;

        let lines = analyze_sql_lineage(sql)?;
        // Expected mappings
        let expected = vec![
            // target1 (CTAS)
            "db1.target1.id <- db1.source_a.id".to_string(),
            "db1.target1.name_upper <- db1.source_a.name".to_string(),
            "db1.target1.sum_v <- db1.source_a.v1, db1.source_a.v2".to_string(),
            "db1.target1.one <- db1.source_a.1".to_string(),
            "db1.target1.ts <- db1.source_a.current_timestamp()".to_string(),
            // target2 (INSERT INTO)
            "db1.target2.id <- db1.source_a.id".to_string(),
            "db1.target2.user_name <- db1.source_b.name".to_string(),
            "db1.target2.total_amount <- db1.source_b.amount".to_string(),
            // target3 (INSERT OVERWRITE TABLE)
            "db2.target3.id <- db1.source_a.id".to_string(),
            "db2.target3.newname <- db1.source_a.name".to_string(),
        ];

        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_ctas_without_use_requires_qualified_names() {
        let sql = r#"
            CREATE TABLE dbx.t1 AS SELECT 1 as one;
        "#;
        let lines = analyze_sql_lineage(sql).unwrap();
        assert_eq!(lines, vec!["dbx.t1.one <- 1".to_string()]);
    }

    #[test]
    fn test_insert_without_column_list_uses_schema() -> Result<()> {
        let sql = r#"
            USE dbs;
            CREATE TABLE s1 (c1 INT, c2 STRING);
            CREATE TABLE t1 (c1 INT, c2 STRING);
            INSERT INTO t1 SELECT c1, c2 FROM s1;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "dbs.t1.c1 <- dbs.s1.c1".to_string(),
            "dbs.t1.c2 <- dbs.s1.c2".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_insert_with_cte_basic() -> Result<()> {
        let sql = r#"
            USE db1;
            CREATE TABLE source_a (id INT, name STRING);
            CREATE TABLE t_cte (id INT, one INT);
            INSERT INTO t_cte
            WITH t AS (
                SELECT id, 1 AS one FROM source_a
            )
            SELECT t.id, t.one FROM t;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "db1.t_cte.id <- db1.source_a.id".to_string(),
            "db1.t_cte.one <- db1.source_a.1".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_insert_with_chained_ctes() -> Result<()> {
        let sql = r#"
            USE db1;
            CREATE TABLE a (id INT);
            CREATE TABLE t2 (id INT);
            INSERT INTO t2 
            WITH t1 AS (SELECT id FROM a),
                 t2c AS (SELECT id FROM t1)
            SELECT id FROM t2c;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec!["db1.t2.id <- db1.a.id".to_string()];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_insert_with_cte_explicit_column_list() -> Result<()> {
        let sql = r#"
            USE db1;
            CREATE TABLE s (id INT, nm STRING);
            CREATE TABLE t3 (x INT, y STRING);
            INSERT INTO t3 
            WITH c (x, y) AS (SELECT id, nm FROM s)
            SELECT x, y FROM c;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "db1.t3.x <- db1.s.id".to_string(),
            "db1.t3.y <- db1.s.nm".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_insert_with_derived_basic() -> Result<()> {
        let sql = r#"
            USE db1;
            CREATE TABLE s (id INT, v INT);
            CREATE TABLE tgt (id INT, x INT);
            INSERT INTO tgt
            SELECT d.id, d.x FROM (SELECT id, v + 1 AS x FROM s) d;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "db1.tgt.id <- db1.s.id".to_string(),
            "db1.tgt.x <- db1.s.v".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_insert_with_derived_alias_columns() -> Result<()> {
        let sql = r#"
            USE db1;
            CREATE TABLE s (id INT);
            CREATE TABLE tgt (id2 INT, n1 INT);
            INSERT INTO tgt
            SELECT d.id2, d.n1 FROM (SELECT id AS id2, 1 AS n1 FROM s) d;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "db1.tgt.id2 <- db1.s.id".to_string(),
            "db1.tgt.n1 <- db1.s.1".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_create_view_lineage_and_explicit_columns() -> Result<()> {
        let sql = r#"
            USE db1;
            CREATE TABLE s (id INT, name STRING);
            CREATE VIEW v1 AS SELECT id, upper(name) AS uname FROM s;
            CREATE VIEW v2 (x, y) AS SELECT id, name FROM s;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "db1.v1.id <- db1.s.id".to_string(),
            "db1.v1.uname <- db1.s.name".to_string(),
            "db1.v2.x <- db1.s.id".to_string(),
            "db1.v2.y <- db1.s.name".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_derived_constant_function_expr_fallback() -> Result<()> {
        let sql = r#"
            USE dbfn;
            CREATE TABLE s (id INT);
            CREATE TABLE tgt (u STRING);
            INSERT INTO tgt
            SELECT d.n FROM (SELECT upper('abc') AS n FROM s) d;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec!["dbfn.tgt.u <- dbfn.s.upper('abc')".to_string()];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_derived_literal_in_list_expr_fallback() -> Result<()> {
        let sql = r#"
            USE dblit;
            CREATE TABLE s (id INT);
            CREATE TABLE tgt (f BOOLEAN);
            INSERT INTO tgt
            SELECT d.f FROM (SELECT 2 IN (1, 2, 3) AS f FROM s) d;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec!["dblit.tgt.f <- dblit.s.2 IN (1, 2, 3)".to_string()];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_window_functions_lineage_in_ctas() -> Result<()> {
        let sql = r#"
            USE dbw3;
            CREATE TABLE s (id INT, v INT, ts TIMESTAMP);
            CREATE TABLE t AS
            SELECT
              sum(v) OVER (PARTITION BY id ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s1,
              count(v) FILTER (WHERE v > 0) OVER (PARTITION BY id ORDER BY ts) AS c1
            FROM s;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            // s1 depends on v (arg), id (partition), ts (order)
            "dbw3.t.s1 <- dbw3.s.id, dbw3.s.ts, dbw3.s.v".to_string(),
            // c1 depends on v (arg), v (filter), id (partition), ts (order) -> unique set is id, ts, v
            "dbw3.t.c1 <- dbw3.s.id, dbw3.s.ts, dbw3.s.v".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_complex_case_constant_in_derived() -> Result<()> {
        let sql = r#"
            USE dbx1;
            CREATE TABLE s (id INT);
            CREATE TABLE tgt (c INT);
            INSERT INTO tgt
            SELECT d.c FROM (SELECT CASE WHEN 1 = 1 THEN 2 ELSE 3 END AS c FROM s) d;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec!["dbx1.tgt.c <- dbx1.s.CASE WHEN 1 = 1 THEN 2 ELSE 3 END".to_string()];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_nested_function_constant_in_derived() -> Result<()> {
        let sql = r#"
            USE dbx2;
            CREATE TABLE s (id INT);
            CREATE TABLE tgt (n STRING);
            INSERT INTO tgt
            SELECT d.n FROM (SELECT upper(lower('AbC')) AS n FROM s) d;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec!["dbx2.tgt.n <- dbx2.s.upper(lower('AbC'))".to_string()];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_window_function_with_constant_mix() -> Result<()> {
        let sql = r#"
            USE dbx3;
            CREATE TABLE s (id INT, v INT, ts TIMESTAMP);
            CREATE TABLE t AS
            SELECT
              sum(v) OVER (PARTITION BY id ORDER BY ts) + 1 AS s2
            FROM s;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            // s2 depends on v (arg), id (partition), ts (order); constant +1 不引入列
            "dbx3.t.s2 <- dbx3.s.id, dbx3.s.ts, dbx3.s.v".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_lineage_detailed_constant_single_vs_multi_table() -> Result<()> {
        let sql = r#"
            USE dbjd;
            CREATE TABLE s (id INT);
            CREATE TABLE a (id INT);
            CREATE TABLE b (id INT);
            -- single-source constant in CTAS
            CREATE TABLE t1 AS SELECT 1 AS c FROM s;
            -- multi-source constant in CTAS
            CREATE TABLE t2 AS SELECT 1 AS c FROM a JOIN b ON a.id = b.id;
        "#;
        let infos = analyze_sql_lineage_detailed(sql)?;
        // find t1.c and t2.c
        let t1 = infos
            .iter()
            .find(|i| i.stmt_type == "CTAS" && i.target == "dbjd.t1" && i.column == "c")
            .unwrap();
        assert!(t1.sources.as_ref().unwrap().iter().any(|s| s == "dbjd.s.1"));
        assert!(t1.expr.is_none());

        let t2 = infos
            .iter()
            .find(|i| i.stmt_type == "CTAS" && i.target == "dbjd.t2" && i.column == "c")
            .unwrap();
        assert!(t2.sources.is_none());
        assert_eq!(t2.expr.as_deref(), Some("1"));
        Ok(())
    }

    #[test]
    fn test_lineage_detailed_json_api() -> Result<()> {
        let sql = r#"
            USE dx;
            CREATE TABLE s (id INT, v INT);
            CREATE TABLE t AS SELECT id as id, v+1 as v2 FROM s;
            INSERT INTO t SELECT id as id, v+1 as v2 FROM s;
        "#;
        let infos = analyze_sql_lineage_detailed(sql)?;
        // expect 4 entries: 2 for CTAS, 2 for INSERT
        assert_eq!(infos.len(), 4);
        // Check one CTAS id mapping
        let ct = infos
            .iter()
            .find(|i| i.stmt_type == "CTAS" && i.column == "id")
            .unwrap();
        assert_eq!(ct.target, "dx.t");
        assert!(ct.sources.as_ref().unwrap().iter().any(|s| s == "dx.s.id"));
        Ok(())
    }

    #[test]
    fn test_ctas_expand_wildcard_single_table() -> Result<()> {
        let sql = r#"
            USE dbw;
            CREATE TABLE s (c1 INT, c2 STRING);
            CREATE TABLE t AS SELECT * FROM s;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "dbw.t.c1 <- dbw.s.c1".to_string(),
            "dbw.t.c2 <- dbw.s.c2".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_ctas_expand_qualified_wildcards_join() -> Result<()> {
        let sql = r#"
            USE dbw2;
            CREATE TABLE a (id INT, x INT);
            CREATE TABLE b (id INT, y INT);
            CREATE TABLE t2 AS SELECT a.*, b.* FROM a JOIN b ON a.id=b.id;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "dbw2.t2.id <- dbw2.a.id".to_string(),
            "dbw2.t2.x <- dbw2.a.x".to_string(),
            "dbw2.t2.id <- dbw2.b.id".to_string(),
            "dbw2.t2.y <- dbw2.b.y".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_analyze_sql_tables_segments() -> Result<()> {
        let sql = r#"
            USE d1;
            SET hive.exec.dynamic.partition=true;
            CREATE TABLE a (id INT);
            CREATE TABLE b (id INT);
            CREATE TABLE t AS SELECT a.id FROM a JOIN b ON a.id=b.id;
            INSERT INTO t SELECT id FROM a;
            CREATE VIEW v AS SELECT a.id FROM a;
        "#;
        let lines = analyze_sql_tables(sql)?;
        let expected = vec![
            "d1.t <- d1.a, d1.b".to_string(),
            "d1.t <- d1.a".to_string(),
            "d1.v <- d1.a".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_create_table_like_no_lineage() -> Result<()> {
        let sql = r#"
            USE db_lk;
            CREATE TABLE s (id INT);
            CREATE TABLE t LIKE s;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        assert_eq!(
            lines.len(),
            0,
            "CREATE TABLE LIKE should not produce lineage lines"
        );
        Ok(())
    }

    #[test]
    fn test_insert_into_table_keyword() -> Result<()> {
        let sql = r#"
            USE dbit;
            CREATE TABLE s (id INT);
            CREATE TABLE t (id INT);
            INSERT INTO TABLE t SELECT id FROM s;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec!["dbit.t.id <- dbit.s.id".to_string()];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_nested_derived_subqueries() -> Result<()> {
        let sql = r#"
            USE dbn;
            CREATE TABLE s (id INT, v INT);
            CREATE TABLE tgt (id INT);
            INSERT INTO tgt
            SELECT d1.id FROM (SELECT id FROM (SELECT id, v FROM s) t0) d1;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec!["dbn.tgt.id <- dbn.s.id".to_string()];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }
}
