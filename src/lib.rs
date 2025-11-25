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
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::rc::Rc;

// Diesel schema and models
pub mod models;
pub mod schema;

// HTTP service modules
pub mod config;
pub mod db;
pub mod handlers;
pub mod server;

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
    sources: HashMap<String, Rc<BTreeSet<ColumnRef>>>,
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
    sources: HashMap<String, Rc<BTreeSet<ColumnRef>>>,
    exprs: HashMap<String, String>,
    // index-preserving slots to handle duplicate output names
    sources_by_index: Vec<Option<Rc<BTreeSet<ColumnRef>>>>,
    exprs_by_index: Vec<Option<String>>,
}

/// Indicates a single, mutually-exclusive default resolution scope for bare identifiers.
/// Only one of these can be active at a time: a physical table, a CTE, or a derived subquery.
#[derive(Debug, Copy, Clone)]
enum DefaultOnly<'a> {
    Table(&'a TableKey),
    Cte(&'a str),
    Derived(&'a str),
}

/// Shared resolution context threaded through expression walkers.
/// Bundles all maps and options to keep function signatures small and consistent.
#[derive(Debug, Copy, Clone)]
struct ResolveCtx<'a> {
    alias_map: &'a HashMap<String, TableKey>,
    cte_aliases: &'a HashMap<String, String>,
    derived_aliases: &'a HashMap<String, DerivedInfo>,
    cte_defs: &'a CteDefs,
    named_windows: Option<&'a HashMap<String, ast::WindowSpec>>,
    default_only: Option<DefaultOnly<'a>>,
}

/// Convenience constructor for `ResolveCtx`.
fn make_resolve_ctx<'a>(
    alias_map: &'a HashMap<String, TableKey>,
    cte_aliases: &'a HashMap<String, String>,
    derived_aliases: &'a HashMap<String, DerivedInfo>,
    cte_defs: &'a CteDefs,
    named_windows: Option<&'a HashMap<String, ast::WindowSpec>>,
    default_only: Option<DefaultOnly<'a>>,
) -> ResolveCtx<'a> {
    ResolveCtx {
        alias_map,
        cte_aliases,
        derived_aliases,
        cte_defs,
        named_windows,
        default_only,
    }
}

/// Returns whether the projection item can fall back to base tables when
/// no concrete sources were found (e.g. unnamed simple expressions).
fn allow_base_fallback_for_item(item: &SelectItem, rctx: &ResolveCtx) -> bool {
    let mut allow = matches!(item, SelectItem::UnnamedExpr(_));
    // Allow fallback for aliased simple identifiers too, e.g., `n AS name`
    if let SelectItem::ExprWithAlias { expr, .. } = item {
        match expr {
            Expr::Identifier(_) => allow = true,
            Expr::CompoundIdentifier(ids) if ids.len() == 1 => allow = true,
            Expr::CompoundIdentifier(ids) if ids.len() == 2 => {
                let qual = &ids[0].value;
                let col = &ids[1].value;
                if let Some(info) = rctx.derived_aliases.get(qual) {
                    if info.exprs.contains_key(col) {
                        allow = false;
                    }
                }
                if let Some(cte_name) = rctx.cte_aliases.get(qual) {
                    if let Some(info) = rctx.cte_defs.defs.get(cte_name) {
                        if info.exprs.contains_key(col) {
                            allow = false;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if let SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) = item {
        if idents.len() == 2 {
            let qual = &idents[0].value;
            let col = &idents[1].value;
            if let Some(info) = rctx.derived_aliases.get(qual) {
                if info.exprs.contains_key(col) {
                    allow = false;
                }
            }
            if let Some(cte_name) = rctx.cte_aliases.get(qual) {
                if let Some(info) = rctx.cte_defs.defs.get(cte_name) {
                    if info.exprs.contains_key(col) {
                        allow = false;
                    }
                }
            }
        }
    }
    allow
}

/// If the item is not from a wildcard and sources are empty, try to fill sources by:
/// 1) Using the default table scope when present; otherwise
/// 2) Falling back to all base tables in scope (when `allow_base_fallback` is true).
///    Returns whether any sources were added.
fn maybe_fill_sources_for_empty(
    item: &SelectItem,
    is_from_wildcard: bool,
    rctx: &ResolveCtx,
    sources: &mut BTreeSet<ColumnRef>,
    allow_base_fallback: bool,
    base_tables_pre: Option<&BTreeSet<TableKey>>,
) -> Result<bool> {
    let mut added = false;
    if !is_from_wildcard && sources.is_empty() {
        if let Some(DefaultOnly::Table(single_tbl)) = rctx.default_only {
            let expr_sql = select_item_expr_sql_with_ctx(item, rctx)?;
            sources.insert(ColumnRef {
                table: (*single_tbl).clone(),
                column: expr_sql,
            });
            added = true;
        } else if allow_base_fallback && allow_base_fallback_for_item(item, rctx) {
            // Prefer base tables from a single derived alias in scope, if present,
            // to better attribute bare identifiers to that subquery's sources.
            let preferred_from_single_derived: Option<BTreeSet<TableKey>> =
                if rctx.derived_aliases.len() == 1 {
                    let info = rctx.derived_aliases.values().next().unwrap();
                    let mut set: BTreeSet<TableKey> = BTreeSet::new();
                    for srcs in info.sources.values() {
                        for s in srcs.iter() {
                            set.insert(s.table.clone());
                        }
                    }
                    if !set.is_empty() {
                        Some(set)
                    } else {
                        None
                    }
                } else {
                    None
                };

            let base_tables: Cow<'_, BTreeSet<TableKey>> =
                if let Some(pref) = preferred_from_single_derived {
                    // Union preferred derived sources with all other base tables visible in scope
                    let mut union_set: BTreeSet<TableKey> = pref;
                    if let Some(pre) = base_tables_pre {
                        for t in pre {
                            union_set.insert(t.clone());
                        }
                    } else {
                        let others = collect_base_tables_in_scope(
                            rctx.alias_map,
                            rctx.cte_aliases,
                            rctx.derived_aliases,
                            rctx.cte_defs,
                        );
                        for t in others {
                            union_set.insert(t);
                        }
                    }
                    Cow::Owned(union_set)
                } else if let Some(pre) = base_tables_pre {
                    Cow::Borrowed(pre)
                } else {
                    Cow::Owned(collect_base_tables_in_scope(
                        rctx.alias_map,
                        rctx.cte_aliases,
                        rctx.derived_aliases,
                        rctx.cte_defs,
                    ))
                };
            if !base_tables.is_empty() {
                // Prefer to propagate bare identifier name when possible (e.g. `id` -> t1.id, t2.id)
                let fallback_col = match item {
                    SelectItem::ExprWithAlias { expr, .. } | SelectItem::UnnamedExpr(expr) => {
                        match expr {
                            Expr::Identifier(ident) => Some(ident_to_string(ident)),
                            Expr::CompoundIdentifier(idents) if idents.len() == 1 => {
                                Some(ident_to_string(&idents[0]))
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                }
                .unwrap_or_else(|| "*".to_string());

                *sources = base_tables
                    .iter()
                    .cloned()
                    .map(|t| ColumnRef {
                        table: t,
                        column: fallback_col.clone(),
                    })
                    .collect();
                added = true;
            }
        }
    }
    Ok(added)
}

fn compute_base_tables_in_scope(rctx: &ResolveCtx) -> BTreeSet<TableKey> {
    collect_base_tables_in_scope(
        rctx.alias_map,
        rctx.cte_aliases,
        rctx.derived_aliases,
        rctx.cte_defs,
    )
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

                    let cte_defs = build_cte_defs(&ctx, q)?;
                    let info = analyze_query_info(&ctx, q, &cte_defs)?;
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
                    let cte_defs = build_cte_defs(&ctx, source_q)?;
                    let info = analyze_query_info(&ctx, source_q, &cte_defs)?;
                    info.columns.clone()
                };

                let source_q = ins
                    .source
                    .as_ref()
                    .ok_or_else(|| anyhow!("INSERT missing source SELECT"))?;
                let cte_defs = build_cte_defs(&ctx, source_q)?;
                let info = analyze_query_info(&ctx, source_q, &cte_defs)?;
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
                let cte_defs = build_cte_defs(&ctx, &query)?;
                let info = analyze_query_info(&ctx, &query, &cte_defs)?;
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
                    let cte_defs = build_cte_defs(&ctx, q)?;
                    let sources = collect_base_tables_in_query(&ctx, q, &cte_defs)?;
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
                    let cte_defs = build_cte_defs(&ctx, source_q)?;
                    let sources = collect_base_tables_in_query(&ctx, source_q, &cte_defs)?;
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
                let cte_defs = build_cte_defs(&ctx, &query)?;
                let sources = collect_base_tables_in_query(&ctx, &query, &cte_defs)?;
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
                    let cte_defs = build_cte_defs(&ctx, q)?;
                    let sources = collect_base_tables_in_query(&ctx, q, &cte_defs)?;
                    let srcs: Vec<String> = sources
                        .iter()
                        .map(|t| format!("{}.{}", t.db, t.table))
                        .collect();
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
                    let cte_defs = build_cte_defs(&ctx, source_q)?;
                    let sources = collect_base_tables_in_query(&ctx, source_q, &cte_defs)?;
                    let srcs: Vec<String> = sources
                        .iter()
                        .map(|t| format!("{}.{}", t.db, t.table))
                        .collect();
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
                let cte_defs = build_cte_defs(&ctx, &query)?;
                let sources = collect_base_tables_in_query(&ctx, &query, &cte_defs)?;
                let srcs: Vec<String> = sources
                    .iter()
                    .map(|t| format!("{}.{}", t.db, t.table))
                    .collect();
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

                    let cte_defs = build_cte_defs(&ctx, q)?;
                    let info = analyze_query_info(&ctx, q, &cte_defs)?;
                    let target_cols: Vec<String> = if !columns.is_empty() {
                        columns.iter().map(|c| ident_to_string(&c.name)).collect()
                    } else {
                        info.columns.clone()
                    };
                    if target_cols.len() != info.columns.len() {
                        bail!(
                            "Target columns count ({}) does not match SELECT projection ({}) for table {}.{}",
                            target_cols.len(),
                            info.columns.len(),
                            target_table.db,
                            target_table.table
                        );
                    }
                    for (i, col_name) in target_cols.iter().enumerate().take(info.columns.len()) {
                        if let Some(Some(srcs)) = info.sources_by_index.get(i) {
                            let srcs_vec: Vec<String> =
                                srcs.iter().map(|c| c.to_string()).collect();
                            out.push(ColumnLineageInfo {
                                stmt_index: idx,
                                stmt_type: "CTAS".to_string(),
                                start_line: span.start.line as u32,
                                start_col: span.start.column as u32,
                                end_line: span.end.line as u32,
                                end_col: span.end.column as u32,
                                target: format!("{}.{}", target_table.db, target_table.table),
                                column: col_name.clone(),
                                sources: Some(srcs_vec),
                                expr: None,
                                statement: extract_snippet(sql, span),
                            });
                        } else if let Some(Some(expr)) = info.exprs_by_index.get(i) {
                            out.push(ColumnLineageInfo {
                                stmt_index: idx,
                                stmt_type: "CTAS".to_string(),
                                start_line: span.start.line as u32,
                                start_col: span.start.column as u32,
                                end_line: span.end.line as u32,
                                end_col: span.end.column as u32,
                                target: format!("{}.{}", target_table.db, target_table.table),
                                column: col_name.clone(),
                                sources: None,
                                expr: Some(expr.clone()),
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
                    let cte_defs = build_cte_defs(&ctx, source_q)?;
                    let info = analyze_query_info(&ctx, source_q, &cte_defs)?;
                    // target columns list similar于 analyze_sql_lineage
                    let target_cols: Vec<String> = if !ins.columns.is_empty() {
                        ins.columns.iter().map(ident_to_string).collect()
                    } else if let Some(cols) = ctx.schemas.get(&target_table) {
                        cols.clone()
                    } else {
                        info.columns.clone()
                    };
                    if target_cols.len() != info.columns.len() {
                        bail!(
                            "Target columns count ({}) does not match SELECT projection ({}) for table {}.{}",
                            target_cols.len(),
                            info.columns.len(),
                            target_table.db,
                            target_table.table
                        );
                    }
                    for (i, col_name) in target_cols.iter().enumerate().take(info.columns.len()) {
                        if let Some(Some(srcs)) = info.sources_by_index.get(i) {
                            let srcs_vec: Vec<String> =
                                srcs.iter().map(|c| c.to_string()).collect();
                            out.push(ColumnLineageInfo {
                                stmt_index: idx,
                                stmt_type: "INSERT".to_string(),
                                start_line: span.start.line as u32,
                                start_col: span.start.column as u32,
                                end_line: span.end.line as u32,
                                end_col: span.end.column as u32,
                                target: format!("{}.{}", target_table.db, target_table.table),
                                column: col_name.clone(),
                                sources: Some(srcs_vec),
                                expr: None,
                                statement: extract_snippet(sql, span),
                            });
                        } else if let Some(Some(expr)) = info.exprs_by_index.get(i) {
                            out.push(ColumnLineageInfo {
                                stmt_index: idx,
                                stmt_type: "INSERT".to_string(),
                                start_line: span.start.line as u32,
                                start_col: span.start.column as u32,
                                end_line: span.end.line as u32,
                                end_col: span.end.column as u32,
                                target: format!("{}.{}", target_table.db, target_table.table),
                                column: col_name.clone(),
                                sources: None,
                                expr: Some(expr.clone()),
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
                let cte_defs = build_cte_defs(&ctx, &query)?;
                let info = analyze_query_info(&ctx, &query, &cte_defs)?;
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
                for (i, col_name) in target_cols.iter().enumerate().take(info.columns.len()) {
                    if let Some(Some(srcs)) = info.sources_by_index.get(i) {
                        let srcs_vec: Vec<String> = srcs.iter().map(|c| c.to_string()).collect();
                        out.push(ColumnLineageInfo {
                            stmt_index: idx,
                            stmt_type: "CREATE_VIEW".to_string(),
                            start_line: span.start.line as u32,
                            start_col: span.start.column as u32,
                            end_line: span.end.line as u32,
                            end_col: span.end.column as u32,
                            target: format!("{}.{}", target_table.db, target_table.table),
                            column: col_name.clone(),
                            sources: Some(srcs_vec),
                            expr: None,
                            statement: extract_snippet(sql, span),
                        });
                    } else if let Some(Some(expr)) = info.exprs_by_index.get(i) {
                        out.push(ColumnLineageInfo {
                            stmt_index: idx,
                            stmt_type: "CREATE_VIEW".to_string(),
                            start_line: span.start.line as u32,
                            start_col: span.start.column as u32,
                            end_line: span.end.line as u32,
                            end_col: span.end.column as u32,
                            target: format!("{}.{}", target_table.db, target_table.table),
                            column: col_name.clone(),
                            sources: None,
                            expr: Some(expr.clone()),
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

// Determine the single default source (table, CTE, or derived) for bare column references.
// Returns None if there are multiple sources or mixed types.
fn determine_default_only<'a>(
    alias_map: &'a HashMap<String, TableKey>,
    cte_aliases: &'a HashMap<String, String>,
    derived_aliases: &'a HashMap<String, DerivedInfo>,
) -> Option<DefaultOnly<'a>> {
    // Priority: Table > CTE > Derived
    // Only return a default if exactly one type has exactly one unique source

    if cte_aliases.is_empty() && derived_aliases.is_empty() {
        // Only physical tables present
        let unique_tables: BTreeSet<_> = alias_map.values().collect();
        if unique_tables.len() == 1 {
            return unique_tables.iter().next().map(|t| DefaultOnly::Table(t));
        }
    } else if alias_map.is_empty() && derived_aliases.is_empty() {
        // Only CTEs present
        let unique_ctes: BTreeSet<_> = cte_aliases.values().collect();
        if unique_ctes.len() == 1 {
            return unique_ctes
                .iter()
                .next()
                .map(|c| DefaultOnly::Cte(c.as_str()));
        }
    } else if alias_map.is_empty() && cte_aliases.is_empty() {
        // Only derived tables present
        if derived_aliases.len() == 1 {
            return derived_aliases
                .keys()
                .next()
                .map(|d| DefaultOnly::Derived(d.as_str()));
        }
    }

    None
}

fn qualify_table(ctx: &Context, name: &ObjectName) -> Result<(String, String)> {
    let parts: Vec<String> = object_name_parts(name)?;
    match parts.len() {
        1 => {
            // Default to "default" database when not set via USE
            let db = ctx
                .current_db
                .clone()
                .unwrap_or_else(|| "default".to_string());
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

// (removed) expect_simple_select: previously used before set operation support

// Analyze a full Query (not just a simple Select), supporting set operations
fn analyze_query_info(ctx: &Context, query: &Query, cte_defs: &CteDefs) -> Result<DerivedInfo> {
    analyze_setexpr_info(ctx, query.body.as_ref(), cte_defs)
}

fn analyze_setexpr_info(ctx: &Context, body: &SetExpr, cte_defs: &CteDefs) -> Result<DerivedInfo> {
    match body {
        SetExpr::Select(select) => analyze_select_info(ctx, select, cte_defs),
        SetExpr::Query(inner) => analyze_setexpr_info(ctx, inner.body.as_ref(), cte_defs),
        SetExpr::SetOperation { left, right, .. } => {
            // detail with union/intersect/except sql
            let left_info = analyze_setexpr_info(ctx, left.as_ref(), cte_defs)?;
            let right_info = analyze_setexpr_info(ctx, right.as_ref(), cte_defs)?;
            merge_derived_info_for_set(left_info, right_info)
        }
        other => bail!("Unsupported query in lineage analysis: {}", other),
    }
}

// Merge two DerivedInfo results coming from a set operation (e.g., UNION/INTERSECT/EXCEPT)
// Column names are taken from the left side (SQL semantics), and per-index sources are
// combined (union). If neither side has concrete sources for a column, keep the left expr
// (or right if left missing).
fn merge_derived_info_for_set(left: DerivedInfo, right: DerivedInfo) -> Result<DerivedInfo> {
    if left.columns.len() != right.columns.len() {
        bail!(
            "Set operation branches have different column counts: {} vs {}",
            left.columns.len(),
            right.columns.len()
        );
    }

    let col_len = left.columns.len();
    let mut out = DerivedInfo {
        columns: left.columns.clone(),
        sources: HashMap::with_capacity(col_len),
        exprs: HashMap::with_capacity(col_len),
        sources_by_index: vec![None; col_len],
        exprs_by_index: vec![None; col_len],
    };
    for i in 0..col_len {
        let mut merged: BTreeSet<ColumnRef> = BTreeSet::new();
        if let Some(Some(ls)) = left.sources_by_index.get(i) {
            merged.extend(ls.iter().cloned());
        }
        if let Some(Some(rs)) = right.sources_by_index.get(i) {
            merged.extend(rs.iter().cloned());
        }
        if !merged.is_empty() {
            let rc_merged = Rc::new(merged);
            out.sources_by_index[i] = Some(rc_merged.clone());
            out.exprs_by_index[i] = None;
            out.sources.insert(out.columns[i].clone(), rc_merged);
        } else {
            let expr = left
                .exprs_by_index
                .get(i)
                .and_then(|e| e.clone())
                .or_else(|| right.exprs_by_index.get(i).and_then(|e| e.clone()))
                .unwrap_or_else(|| "".to_string());
            if !expr.is_empty() {
                out.exprs_by_index[i] = Some(expr.clone());
                out.sources_by_index[i] = None;
                out.exprs.insert(out.columns[i].clone(), expr);
            }
        }
    }
    Ok(out)
}

// Collect base tables used by a Query (including set operations)
fn collect_base_tables_in_query(
    ctx: &Context,
    query: &Query,
    cte_defs: &CteDefs,
) -> Result<BTreeSet<TableKey>> {
    collect_base_tables_in_setexpr(ctx, query.body.as_ref(), cte_defs)
}

fn collect_base_tables_in_setexpr(
    ctx: &Context,
    body: &SetExpr,
    cte_defs: &CteDefs,
) -> Result<BTreeSet<TableKey>> {
    match body {
        SetExpr::Select(select) => {
            let (alias_map, cte_aliases, derived_aliases) =
                build_alias_map_with_cte(ctx, &select.from, cte_defs)?;
            Ok(collect_base_tables_in_scope(
                &alias_map,
                &cte_aliases,
                &derived_aliases,
                cte_defs,
            ))
        }
        SetExpr::Query(inner) => collect_base_tables_in_setexpr(ctx, inner.body.as_ref(), cte_defs),
        SetExpr::SetOperation { left, right, .. } => {
            let mut set = collect_base_tables_in_setexpr(ctx, left.as_ref(), cte_defs)?;
            let right_set = collect_base_tables_in_setexpr(ctx, right.as_ref(), cte_defs)?;
            for t in right_set {
                set.insert(t);
            }
            Ok(set)
        }
        other => bail!("Unsupported query for table lineage: {}", other),
    }
}

fn list_sources_in_order(from: &[TableWithJoins], cte_defs: &CteDefs) -> Vec<(String, SourceKind)> {
    // Return a vec of (qualifier, SourceKind) in FROM order
    let est = from.iter().map(|twj| 1 + twj.joins.len()).sum();
    let mut v = Vec::with_capacity(est);
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
    let mut out: Vec<ExpandedItem> = Vec::with_capacity(select.projection.len());
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

// deprecated: replaced by compute_group_having_cols(select, rctx)
// fn union_group_having(select: &Select, rctx: &ResolveCtx, acc: &mut BTreeSet<ColumnRef>) { }

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
                for s in srcs.iter() {
                    set.insert(s.table.clone());
                }
            }
        }
    }
    for info in derived_aliases.values() {
        for srcs in info.sources.values() {
            for s in srcs.iter() {
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
    let default_only = determine_default_only(&alias_map, &cte_aliases, &derived_aliases);

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
        sources: HashMap::with_capacity(cols.len()),
        exprs: HashMap::with_capacity(cols.len()),
        sources_by_index: vec![None; cols.len()],
        exprs_by_index: vec![None; cols.len()],
    };
    let named_windows = build_named_window_map(select);

    let rctx = make_resolve_ctx(
        &alias_map,
        &cte_aliases,
        &derived_aliases,
        cte_defs,
        Some(&named_windows),
        default_only,
    );
    // group/having ignored for lineage; no need to compute
    let base_tables_pre = compute_base_tables_in_scope(&rctx);
    for (i, exp) in expanded.iter().enumerate() {
        let (mut sources, mut has_any_col) =
            collect_sources_from_select_item_with_cte(&exp.item, &rctx)?;
        // ignore group by / having for lineage mapping

        if maybe_fill_sources_for_empty(
            &exp.item,
            exp.is_from_wildcard,
            &rctx,
            &mut sources,
            true,
            Some(&base_tables_pre),
        )? {
            has_any_col = true;
        }

        if has_any_col {
            let rc_sources = Rc::new(sources);
            info.sources
                .insert(info.columns[i].clone(), rc_sources.clone());
            info.sources_by_index[i] = Some(rc_sources);
            info.exprs_by_index[i] = None;
        } else {
            let expr_sql = select_item_expr_sql_with_ctx(&exp.item, &rctx)?;
            info.exprs.insert(info.columns[i].clone(), expr_sql.clone());
            info.exprs_by_index[i] = Some(expr_sql);
            info.sources_by_index[i] = None;
        }
    }

    Ok(info)
}

// Simple alias to keep signatures tidy
type AliasMaps = (
    HashMap<String, TableKey>,
    HashMap<String, String>,
    HashMap<String, DerivedInfo>,
);

// Build alias map for physical tables and record CTE aliases used in FROM/JOIN
fn build_alias_map_with_cte(
    ctx: &Context,
    from: &[TableWithJoins],
    cte_defs: &CteDefs,
) -> Result<AliasMaps> {
    let est = from.iter().map(|twj| 1 + twj.joins.len()).sum();
    let mut map: HashMap<String, TableKey> = HashMap::with_capacity(est);
    let mut cte_aliases: HashMap<String, String> = HashMap::with_capacity(est);
    let mut derived_aliases: HashMap<String, DerivedInfo> = HashMap::with_capacity(est);

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
            let derived = analyze_query_info(ctx, &cte.query, &defs)?;
            // column names: explicit list if provided; else from derived info
            let col_names: Vec<String> = if !cte.alias.columns.is_empty() {
                cte.alias
                    .columns
                    .iter()
                    .map(|c| ident_to_string(&c.name))
                    .collect()
            } else {
                derived.columns.clone()
            };

            let mut info = CteInfo {
                columns: col_names.clone(),
                sources: HashMap::with_capacity(col_names.len()),
                exprs: HashMap::with_capacity(col_names.len()),
            };
            for (i, name) in col_names.into_iter().enumerate() {
                if let Some(Some(srcs)) = derived.sources_by_index.get(i) {
                    info.sources.insert(name, srcs.clone());
                } else if let Some(Some(expr)) = derived.exprs_by_index.get(i) {
                    info.exprs.insert(name, expr.clone());
                }
            }
            defs.defs.insert(cte_name, info);
        }
    }
    Ok(defs)
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
    // Build info with base-fallback enabled so bare identifiers in multi-table
    // derived queries map to each base table's same-named column.
    let mut info = analyze_query_info(ctx, subquery, cte_defs)?;
    // Apply alias column names if provided
    if let Some(alias) = alias {
        if !alias.columns.is_empty() {
            let new_cols: Vec<String> = alias
                .columns
                .iter()
                .map(|c| ident_to_string(&c.name))
                .collect();
            let mut renamed = DerivedInfo {
                columns: new_cols.clone(),
                sources: HashMap::with_capacity(new_cols.len()),
                exprs: HashMap::with_capacity(new_cols.len()),
                sources_by_index: vec![None; new_cols.len()],
                exprs_by_index: vec![None; new_cols.len()],
            };
            for (i, name) in new_cols.into_iter().enumerate() {
                if let Some(Some(srcs)) = info.sources_by_index.get(i) {
                    renamed.sources_by_index[i] = Some(srcs.clone());
                    renamed.sources.insert(name, srcs.clone());
                } else if let Some(Some(expr)) = info.exprs_by_index.get(i) {
                    renamed.exprs_by_index[i] = Some(expr.clone());
                    renamed.exprs.insert(name, expr.clone());
                }
            }
            info = renamed;
        }
    }
    Ok(info)
}

fn collect_sources_from_select_item_with_cte(
    item: &SelectItem,
    rctx: &ResolveCtx,
) -> Result<(BTreeSet<ColumnRef>, bool)> {
    let expr = match item {
        SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::UnnamedExpr(expr) => expr,
        other => bail!("Unsupported select item in lineage analysis: {}", other),
    };
    let mut set: BTreeSet<ColumnRef> = BTreeSet::new();
    collect_columns_from_expr_with_cte(expr, rctx, &mut set);
    let has_any = !set.is_empty();
    Ok((set, has_any))
}

fn collect_columns_from_expr_with_cte(
    expr: &Expr,
    rctx: &ResolveCtx,
    acc: &mut BTreeSet<ColumnRef>,
) {
    match expr {
        Expr::Identifier(ident) => match rctx.default_only {
            Some(DefaultOnly::Table(tbl)) => {
                acc.insert(ColumnRef {
                    table: tbl.clone(),
                    column: ident_to_string(ident),
                });
            }
            Some(DefaultOnly::Cte(cte_name)) => {
                if let Some(info) = rctx.cte_defs.defs.get(cte_name) {
                    if let Some(srcs) = info.sources.get(&ident.value) {
                        for s in srcs.iter() {
                            acc.insert(s.clone());
                        }
                    }
                }
            }
            Some(DefaultOnly::Derived(derived_name)) => {
                if let Some(info) = rctx.derived_aliases.get(derived_name) {
                    if let Some(srcs) = info.sources.get(&ident.value) {
                        for s in srcs.iter() {
                            acc.insert(s.clone());
                        }
                    }
                }
            }
            None => {}
        },
        Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                let qualifier = &idents[0].value;
                let col = &idents[1].value;
                if let Some(tbl) = rctx.alias_map.get(qualifier) {
                    acc.insert(ColumnRef {
                        table: tbl.clone(),
                        column: col.clone(),
                    });
                } else if let Some(cte_name) = rctx.cte_aliases.get(qualifier) {
                    if let Some(info) = rctx.cte_defs.defs.get(cte_name) {
                        if let Some(srcs) = info.sources.get(col) {
                            for s in srcs.iter() {
                                acc.insert(s.clone());
                            }
                        }
                    }
                } else if let Some(info) = rctx.derived_aliases.get(qualifier) {
                    if let Some(srcs) = info.sources.get(col) {
                        for s in srcs.iter() {
                            acc.insert(s.clone());
                        }
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_columns_from_expr_with_cte(left, rctx, acc);
            collect_columns_from_expr_with_cte(right, rctx, acc);
        }
        Expr::UnaryOp { expr, .. } => collect_columns_from_expr_with_cte(expr, rctx, acc),
        Expr::Nested(e) => collect_columns_from_expr_with_cte(e, rctx, acc),
        Expr::Function(fun) => {
            match &fun.args {
                ast::FunctionArguments::None => {}
                ast::FunctionArguments::Subquery(_) => {}
                ast::FunctionArguments::List(list) => {
                    for arg in &list.args {
                        match arg {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                collect_columns_from_expr_with_cte(e, rctx, acc)
                            }
                            ast::FunctionArg::Named { arg, .. }
                            | ast::FunctionArg::ExprNamed { arg, .. } => {
                                if let ast::FunctionArgExpr::Expr(e) = arg {
                                    collect_columns_from_expr_with_cte(e, rctx, acc)
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
                collect_columns_from_expr_with_cte(f, rctx, acc);
            }
            if let Some(over) = &fun.over {
                match over {
                    ast::WindowType::WindowSpec(spec) => {
                        for e in &spec.partition_by {
                            collect_columns_from_expr_with_cte(e, rctx, acc);
                        }
                        for ob in &spec.order_by {
                            collect_columns_from_expr_with_cte(&ob.expr, rctx, acc);
                        }
                        if let Some(frame) = &spec.window_frame {
                            if let ast::WindowFrameBound::Preceding(Some(n))
                            | ast::WindowFrameBound::Following(Some(n)) = &frame.start_bound
                            {
                                collect_columns_from_expr_with_cte(n, rctx, acc)
                            }
                            if let Some(
                                ast::WindowFrameBound::Preceding(Some(n))
                                | ast::WindowFrameBound::Following(Some(n)),
                            ) = &frame.end_bound
                            {
                                collect_columns_from_expr_with_cte(n, rctx, acc)
                            }
                        }
                    }
                    ast::WindowType::NamedWindow(id) => {
                        if let Some(map) = rctx.named_windows {
                            if let Some(spec) = map.get(&id.value) {
                                for e in &spec.partition_by {
                                    collect_columns_from_expr_with_cte(e, rctx, acc);
                                }
                                for ob in &spec.order_by {
                                    collect_columns_from_expr_with_cte(&ob.expr, rctx, acc);
                                }
                                if let Some(frame) = &spec.window_frame {
                                    if let ast::WindowFrameBound::Preceding(Some(n))
                                    | ast::WindowFrameBound::Following(Some(n)) =
                                        &frame.start_bound
                                    {
                                        collect_columns_from_expr_with_cte(n, rctx, acc)
                                    }
                                    if let Some(
                                        ast::WindowFrameBound::Preceding(Some(n))
                                        | ast::WindowFrameBound::Following(Some(n)),
                                    ) = &frame.end_bound
                                    {
                                        collect_columns_from_expr_with_cte(n, rctx, acc)
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for ob in &fun.within_group {
                collect_columns_from_expr_with_cte(&ob.expr, rctx, acc);
            }
        }
        Expr::Cast { expr, .. } => collect_columns_from_expr_with_cte(expr, rctx, acc),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_columns_from_expr_with_cte(op, rctx, acc);
            }
            for c in conditions {
                collect_columns_from_expr_with_cte(&c.condition, rctx, acc);
                collect_columns_from_expr_with_cte(&c.result, rctx, acc);
            }
            if let Some(e) = else_result {
                collect_columns_from_expr_with_cte(e, rctx, acc);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            collect_columns_from_expr_with_cte(expr, rctx, acc);
            collect_columns_from_expr_with_cte(pattern, rctx, acc);
        }
        Expr::ILike { expr, pattern, .. } => {
            collect_columns_from_expr_with_cte(expr, rctx, acc);
            collect_columns_from_expr_with_cte(pattern, rctx, acc);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_columns_from_expr_with_cte(expr, rctx, acc);
            collect_columns_from_expr_with_cte(low, rctx, acc);
            collect_columns_from_expr_with_cte(high, rctx, acc);
        }
        Expr::InList { expr, list, .. } => {
            collect_columns_from_expr_with_cte(expr, rctx, acc);
            for e in list {
                collect_columns_from_expr_with_cte(e, rctx, acc);
            }
        }
        Expr::InSubquery { expr, .. } => {
            collect_columns_from_expr_with_cte(expr, rctx, acc);
        }
        Expr::GroupingSets(sets) => {
            for group in sets {
                for e in group {
                    collect_columns_from_expr_with_cte(e, rctx, acc);
                }
            }
        }
        Expr::Cube(sets) | Expr::Rollup(sets) => {
            for group in sets {
                for e in group {
                    collect_columns_from_expr_with_cte(e, rctx, acc);
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

fn select_item_expr_sql_with_ctx(item: &SelectItem, rctx: &ResolveCtx) -> Result<String> {
    select_item_expr_sql_with_cte_and_derived(
        item,
        rctx.cte_aliases,
        rctx.derived_aliases,
        rctx.cte_defs,
    )
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

    #[test]
    fn test_insert_with_union_all() -> Result<()> {
        let sql = r#"
            USE d;
            CREATE TABLE sals (id INT, n STRING);
            CREATE TABLE ducks (i INT, j STRING);
            CREATE TABLE tae (id INT, name STRING);
            INSERT OVERWRITE TABLE tae
            SELECT id, n AS name FROM sals
            UNION ALL
            SELECT i, j FROM ducks;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "d.tae.id <- d.ducks.i, d.sals.id".to_string(),
            "d.tae.name <- d.ducks.j, d.sals.n".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_insert_with_union() -> Result<()> {
        let sql = r#"
            USE d2;
            CREATE TABLE sals (id INT, n STRING);
            CREATE TABLE ducks (i INT, j STRING);
            CREATE TABLE tae (id INT, name STRING);
            INSERT INTO tae
            SELECT id, n AS name FROM sals
            UNION
            SELECT i, j FROM ducks;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "d2.tae.id <- d2.ducks.i, d2.sals.id".to_string(),
            "d2.tae.name <- d2.ducks.j, d2.sals.n".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_insert_with_intersect() -> Result<()> {
        let sql = r#"
            USE d3;
            CREATE TABLE sals (id INT, n STRING);
            CREATE TABLE ducks (i INT, j STRING);
            CREATE TABLE tae (id INT, name STRING);
            INSERT INTO tae
            SELECT id, n AS name FROM sals
            INTERSECT
            SELECT i, j FROM ducks;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "d3.tae.id <- d3.ducks.i, d3.sals.id".to_string(),
            "d3.tae.name <- d3.ducks.j, d3.sals.n".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_insert_with_except() -> Result<()> {
        let sql = r#"
            USE d4;
            CREATE TABLE sals (id INT, n STRING);
            CREATE TABLE ducks (i INT, j STRING);
            CREATE TABLE tae (id INT, name STRING);
            INSERT INTO tae
            SELECT id, n AS name FROM sals
            EXCEPT
            SELECT i, j FROM ducks;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "d4.tae.id <- d4.ducks.i, d4.sals.id".to_string(),
            "d4.tae.name <- d4.ducks.j, d4.sals.n".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_tables_lineage_set_ops() -> Result<()> {
        let sql = r#"
            USE dtb;
            CREATE TABLE s (id INT, n STRING);
            CREATE TABLE d (i INT, j STRING);
            CREATE TABLE t AS SELECT id, n FROM s UNION SELECT i, j FROM d;
            INSERT INTO t SELECT id, n FROM s EXCEPT SELECT i, j FROM d;
        "#;
        let lines = analyze_sql_tables(sql)?;
        let expected = vec![
            "dtb.t <- dtb.d, dtb.s".to_string(),
            "dtb.t <- dtb.d, dtb.s".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }

    #[test]
    fn test_lineage_detailed_with_intersect() -> Result<()> {
        let sql = r#"
            USE dj;
            CREATE TABLE sals (id INT, n STRING);
            CREATE TABLE ducks (i INT, j STRING);
            CREATE TABLE tae (id INT, name STRING);
            INSERT INTO tae
            SELECT id, n AS name FROM sals
            INTERSECT
            SELECT i, j FROM ducks;
        "#;
        let infos = analyze_sql_lineage_detailed(sql)?;
        let items: Vec<_> = infos
            .iter()
            .filter(|e| e.stmt_type == "INSERT" && e.target == "dj.tae")
            .collect();
        assert_eq!(items.len(), 2);
        let id = items.iter().find(|e| e.column == "id").unwrap();
        let name = items.iter().find(|e| e.column == "name").unwrap();
        let sid = id.sources.as_ref().unwrap();
        assert!(sid.iter().any(|s| s == "dj.sals.id"));
        assert!(sid.iter().any(|s| s == "dj.ducks.i"));
        let sname = name.sources.as_ref().unwrap();
        assert!(sname.iter().any(|s| s == "dj.sals.n"));
        assert!(sname.iter().any(|s| s == "dj.ducks.j"));
        Ok(())
    }

    #[test]
    fn test_lineage_detailed_with_except() -> Result<()> {
        let sql = r#"
            USE dk;
            CREATE TABLE sals (id INT, n STRING);
            CREATE TABLE ducks (i INT, j STRING);
            CREATE TABLE tae (id INT, name STRING);
            INSERT INTO tae
            SELECT id, n AS name FROM sals
            EXCEPT
            SELECT i, j FROM ducks;
        "#;
        let infos = analyze_sql_lineage_detailed(sql)?;
        let items: Vec<_> = infos
            .iter()
            .filter(|e| e.stmt_type == "INSERT" && e.target == "dk.tae")
            .collect();
        assert_eq!(items.len(), 2);
        let id = items.iter().find(|e| e.column == "id").unwrap();
        let name = items.iter().find(|e| e.column == "name").unwrap();
        let sid = id.sources.as_ref().unwrap();
        assert!(sid.iter().any(|s| s == "dk.sals.id"));
        assert!(sid.iter().any(|s| s == "dk.ducks.i"));
        let sname = name.sources.as_ref().unwrap();
        assert!(sname.iter().any(|s| s == "dk.sals.n"));
        assert!(sname.iter().any(|s| s == "dk.ducks.j"));
        Ok(())
    }

    #[test]
    fn test_bare_identifier_multi_tables_fallback_as_columns() -> Result<()> {
        let sql = r#"
            USE xa;
            CREATE TABLE sals (id INT, n STRING);
            CREATE TABLE ducks (id INT, n STRING);
            CREATE TABLE tae (id INT, name STRING);
            INSERT OVERWRITE TABLE tae SELECT id, n AS name FROM sals JOIN ducks;
        "#;
        let lines = analyze_sql_lineage(sql)?;
        let expected = vec![
            "xa.tae.id <- xa.ducks.id, xa.sals.id".to_string(),
            "xa.tae.name <- xa.ducks.n, xa.sals.n".to_string(),
        ];
        assert_eq!(sorted(lines), sorted(expected));
        Ok(())
    }
}
