use crate::actions::discovery::{Discovery, ServerInfo, ServerStatus};
use crate::actions::Action;
use crate::meta;
use anyhow::Result;
use clap::ValueEnum;
use csv::Writer;
use datafusion::arrow;
use datafusion::arrow::csv::reader::Format;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use indicatif::HumanBytes;
use riffle_server::util;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments, GroupByExpr,
    JoinConstraint, JoinOperator, ObjectName, OrderBy, OrderByKind, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, WindowType,
};
use std::collections::HashSet;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Cursor, Seek, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use url::{ParseError, Url};

const INSTANCES_TABLE_NAME: &str = "instances";
const HISTORICAL_APPS_TABLE_NAME: &str = "historical_apps";
const ACTIVE_APPS_TABLE_NAME: &str = "active_apps";
const RUNNING_APPS_COLUMN_NAME: &str = "running_apps";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct QueryTableRequirements {
    required_tables: Vec<String>,
    include_instance_running_apps: bool,
}

pub struct SessionContextExtend {
    ctx: Arc<SessionContext>,
    discovery: Discovery,
}

impl Deref for SessionContextExtend {
    type Target = Arc<SessionContext>;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TableInstance {
    pub ip: String,
    pub grpc_port: u16,
    pub urpc_port: i32,
    pub http_port: i32,

    pub total_memory: String,
    pub used_memory: String,
    pub running_apps: Option<usize>,
    pub tags: String,
    pub status: ServerStatus,
}

impl From<(&ServerInfo, Option<usize>)> for TableInstance {
    fn from((info, running_apps): (&ServerInfo, Option<usize>)) -> Self {
        Self {
            ip: info.ip.to_string(),
            grpc_port: info.grpc_port,
            urpc_port: info.netty_port,
            http_port: info.http_port as i32,
            total_memory: HumanBytes(info.total_memory as u64).to_string(),
            used_memory: HumanBytes(info.used_memory as u64).to_string(),
            running_apps,
            tags: info
                .tags
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
            status: info.status.clone(),
        }
    }
}

impl SessionContextExtend {
    pub async fn new(coordinator_url: &str, tables: Option<Vec<&str>>) -> Result<Self> {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let self_me = Self {
            ctx: Arc::new(ctx),
            discovery: Discovery::new(&[coordinator_url]),
        };
        self_me.reload(tables).await?;
        Ok(self_me)
    }

    async fn new_with_table_requirements(
        coordinator_url: &str,
        requirements: QueryTableRequirements,
    ) -> Result<Self> {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let self_me = Self {
            ctx: Arc::new(ctx),
            discovery: Discovery::new(&[coordinator_url]),
        };
        self_me.reload_with_table_requirements(requirements).await?;
        Ok(self_me)
    }

    pub fn get_context(&self) -> Arc<SessionContext> {
        self.ctx.clone()
    }

    pub async fn reload(&self, tables: Option<Vec<&str>>) -> Result<()> {
        let requirements = QueryTableRequirements {
            required_tables: tables
                .unwrap_or_default()
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
            include_instance_running_apps: true,
        };
        self.reload_with_table_requirements(requirements).await
    }

    async fn reload_with_table_requirements(
        &self,
        requirements: QueryTableRequirements,
    ) -> Result<()> {
        let tables = &requirements.required_tables;
        let load_all_tables = tables.is_empty();

        if load_all_tables || tables.iter().any(|table| table == INSTANCES_TABLE_NAME) {
            self.register_instances(requirements.include_instance_running_apps)
                .await?;
        }
        if load_all_tables || tables.iter().any(|table| table == ACTIVE_APPS_TABLE_NAME) {
            self.register_active_apps().await?;
        }
        if load_all_tables
            || tables
                .iter()
                .any(|table| table == HISTORICAL_APPS_TABLE_NAME)
        {
            self.register_historical_apps().await?;
        }
        Ok(())
    }

    async fn register_active_apps(&self) -> Result<()> {
        let apps = self.discovery.list_active_apps().await?;
        self.register(apps, ACTIVE_APPS_TABLE_NAME)?;
        Ok(())
    }

    async fn register_historical_apps(&self) -> Result<()> {
        let apps = self.discovery.list_historical_apps().await?;
        self.register(apps, HISTORICAL_APPS_TABLE_NAME)?;
        Ok(())
    }

    async fn register_instances(&self, include_running_apps: bool) -> Result<()> {
        let instances = self.discovery.list_nodes().await?;
        let running_apps = if include_running_apps {
            Some(self.discovery.count_running_apps_per_node().await?)
        } else {
            None
        };
        let instances = instances
            .iter()
            .map(|info| {
                let running_apps = running_apps.as_ref().and_then(|counts| {
                    counts
                        .get(&(info.ip.clone(), info.http_port))
                        .copied()
                        .flatten()
                });
                (info, running_apps).into()
            })
            .collect::<Vec<TableInstance>>();
        self.register(instances, INSTANCES_TABLE_NAME)?;
        Ok(())
    }

    fn register<T>(&self, raw_vec: Vec<T>, table_name: &str) -> Result<()>
    where
        T: Serialize,
    {
        let mut wtr = Writer::from_writer(vec![]);
        for raw in raw_vec {
            wtr.serialize(raw)?;
        }
        let raw_bytes = String::from_utf8(wtr.into_inner()?)?;

        let mut cursor = Cursor::new(raw_bytes);
        let (schema, _) = Format::default()
            .with_header(true)
            .infer_schema(&mut cursor, None)
            .unwrap();
        cursor.rewind()?;
        let schema = Arc::new(schema);

        let mut reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .build(cursor)?;
        let batches = reader.collect::<arrow::error::Result<Vec<_>>>()?;

        let table = MemTable::try_new(schema, vec![batches])?;
        let _ = self.ctx.deregister_table(table_name);
        self.ctx.register_table(table_name, Arc::new(table))?;

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
}

pub struct QueryAction {
    sql: String,
    format: OutputFormat,
    coordinator_url: String,
}

impl QueryAction {
    pub fn new(sql: String, format: OutputFormat, coordinator_name: String) -> Self {
        let url = match Url::parse(coordinator_name.as_str()) {
            Ok(url) => coordinator_name,
            Err(_) => {
                let meta = meta::Metadata::load(None);
                let mapping = meta.get_cluster_ids();
                let url = mapping.get(&coordinator_name);
                if url.is_none() {
                    panic!("Illegal coordinator url/cluster id. {}", &coordinator_name);
                }
                url.unwrap().into()
            }
        };

        Self {
            sql,
            format,
            coordinator_url: url,
        }
    }
}

#[async_trait::async_trait]
impl Action for QueryAction {
    async fn act(&self) -> anyhow::Result<()> {
        let sql = self.sql.as_str();
        let requirements = collect_query_table_requirements(sql);
        let context = SessionContextExtend::new_with_table_requirements(
            self.coordinator_url.as_str(),
            requirements,
        )
        .await?;
        let df = context.sql(sql).await?;
        match &self.format {
            OutputFormat::Table => {
                df.show().await?;
            }
            OutputFormat::Json => {
                let temp_dir = tempfile::tempdir().unwrap();
                let file_path = temp_dir.path().join("output.json");
                let absolute_file_path = file_path.to_str().unwrap();
                df.write_json(absolute_file_path, DataFrameWriteOptions::new(), None)
                    .await?;
                if !file_path.exists() {
                    return Ok(());
                }
                let contents = fs::read_to_string(absolute_file_path)?;
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() || line == "[" || line == "]" {
                        continue;
                    }
                    println!("{}", line.trim_end_matches(','));
                }
            }
        }
        Ok(())
    }
}

fn extract_tables(sql: &str) -> Vec<String> {
    collect_query_table_requirements(sql).required_tables
}

fn collect_query_table_requirements(sql: &str) -> QueryTableRequirements {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).expect("SQL parsing failed");

    let mut requirements = QueryTableRequirements::default();
    for stmt in ast {
        collect_statement_requirements(&stmt, &mut requirements);
    }
    requirements
}

fn collect_statement_requirements(stmt: &Statement, requirements: &mut QueryTableRequirements) {
    match stmt {
        Statement::Query(query) => {
            collect_query_requirements(query, requirements);
        }
        _ => {}
    }
}

fn collect_query_requirements(
    query: &Box<Query>,
    requirements: &mut QueryTableRequirements,
) -> bool {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_requirements(&cte.query, requirements);
        }
    }

    let query_references_instances = match &*query.body {
        SetExpr::Select(select) => collect_select_requirements(select, requirements),
        SetExpr::Query(subquery) => collect_query_requirements(subquery, requirements),
        SetExpr::SetOperation { left, right, .. } => {
            let left_references_instances = collect_set_expr_requirements(left, requirements);
            let right_references_instances = collect_set_expr_requirements(right, requirements);
            left_references_instances || right_references_instances
        }
        _ => false,
    };

    if query_references_instances
        && order_by_references_running_apps(&query.order_by, &HashSet::new())
    {
        requirements.include_instance_running_apps = true;
    }

    query_references_instances
}

fn collect_set_expr_requirements(
    expr: &SetExpr,
    requirements: &mut QueryTableRequirements,
) -> bool {
    match expr {
        SetExpr::Select(select) => collect_select_requirements(select, requirements),
        SetExpr::Query(query) => collect_query_requirements(query, requirements),
        SetExpr::SetOperation { left, right, .. } => {
            let left_references_instances = collect_set_expr_requirements(left, requirements);
            let right_references_instances = collect_set_expr_requirements(right, requirements);
            left_references_instances || right_references_instances
        }
        _ => false,
    }
}

fn collect_select_requirements(select: &Select, requirements: &mut QueryTableRequirements) -> bool {
    let mut instance_table_refs = HashSet::new();
    for table_with_joins in &select.from {
        collect_table_with_joins_requirements(
            table_with_joins,
            requirements,
            &mut instance_table_refs,
        );
    }

    let references_instances = !instance_table_refs.is_empty();
    if references_instances && select_references_running_apps(select, &instance_table_refs) {
        requirements.include_instance_running_apps = true;
    }

    references_instances
}

fn collect_table_with_joins_requirements(
    twj: &TableWithJoins,
    requirements: &mut QueryTableRequirements,
    instance_table_refs: &mut HashSet<String>,
) {
    collect_table_factor_requirements(&twj.relation, requirements, instance_table_refs);
    for join in &twj.joins {
        collect_table_factor_requirements(&join.relation, requirements, instance_table_refs);
        if join_operator_references_running_apps(&join.join_operator, instance_table_refs) {
            requirements.include_instance_running_apps = true;
        }
    }
}

fn collect_table_factor_requirements(
    factor: &TableFactor,
    requirements: &mut QueryTableRequirements,
    instance_table_refs: &mut HashSet<String>,
) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            requirements.required_tables.push(table_name);
            if object_name_matches(name, INSTANCES_TABLE_NAME) {
                instance_table_refs.insert(INSTANCES_TABLE_NAME.to_string());
                if let Some(alias) = alias {
                    instance_table_refs.insert(normalize_sql_identifier(&alias.name.value));
                }
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_query_requirements(subquery, requirements);
        }
        _ => {}
    }
}

fn select_references_running_apps(select: &Select, instance_table_refs: &HashSet<String>) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Wildcard(_) => true,
        SelectItem::QualifiedWildcard(name, _) => match name {
            sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                object_name_matches_reference(name, instance_table_refs)
            }
            sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr) => {
                expr_references_running_apps(expr, instance_table_refs)
            }
        },
        SelectItem::UnnamedExpr(expr) => expr_references_running_apps(expr, instance_table_refs),
        SelectItem::ExprWithAlias { expr, .. } => {
            expr_references_running_apps(expr, instance_table_refs)
        }
    }) || select
        .selection
        .as_ref()
        .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
        || select
            .prewhere
            .as_ref()
            .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
        || select
            .having
            .as_ref()
            .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
        || select
            .qualify
            .as_ref()
            .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
        || group_by_references_running_apps(&select.group_by, instance_table_refs)
        || select
            .cluster_by
            .iter()
            .any(|expr| expr_references_running_apps(expr, instance_table_refs))
        || select
            .distribute_by
            .iter()
            .any(|expr| expr_references_running_apps(expr, instance_table_refs))
        || select
            .sort_by
            .iter()
            .any(|expr| expr_references_running_apps(expr, instance_table_refs))
}

fn group_by_references_running_apps(
    group_by: &GroupByExpr,
    instance_table_refs: &HashSet<String>,
) -> bool {
    match group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .any(|expr| expr_references_running_apps(expr, instance_table_refs)),
    }
}

fn order_by_references_running_apps(
    order_by: &Option<OrderBy>,
    instance_table_refs: &HashSet<String>,
) -> bool {
    order_by
        .as_ref()
        .is_some_and(|order_by| match &order_by.kind {
            OrderByKind::All(_) => true,
            OrderByKind::Expressions(exprs) => exprs
                .iter()
                .any(|expr| expr_references_running_apps(&expr.expr, instance_table_refs)),
        })
}

fn join_operator_references_running_apps(
    join_operator: &JoinOperator,
    instance_table_refs: &HashSet<String>,
) -> bool {
    match join_operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => {
            join_constraint_references_running_apps(constraint, instance_table_refs)
        }
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            expr_references_running_apps(match_condition, instance_table_refs)
                || join_constraint_references_running_apps(constraint, instance_table_refs)
        }
        JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => false,
    }
}

fn join_constraint_references_running_apps(
    constraint: &JoinConstraint,
    instance_table_refs: &HashSet<String>,
) -> bool {
    match constraint {
        JoinConstraint::On(expr) => expr_references_running_apps(expr, instance_table_refs),
        JoinConstraint::Using(names) => names
            .iter()
            .any(|name| object_name_matches(name, RUNNING_APPS_COLUMN_NAME)),
        JoinConstraint::Natural | JoinConstraint::None => false,
    }
}

fn expr_references_running_apps(expr: &Expr, instance_table_refs: &HashSet<String>) -> bool {
    match expr {
        Expr::Identifier(ident) => sql_identifier_eq(&ident.value, RUNNING_APPS_COLUMN_NAME),
        Expr::CompoundIdentifier(idents) => {
            compound_identifier_references_running_apps(idents, instance_table_refs)
        }
        Expr::Nested(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::UnaryOp { expr, .. }
        | Expr::OuterJoin(expr)
        | Expr::Prior(expr) => expr_references_running_apps(expr, instance_table_refs),
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => {
            expr_references_running_apps(left, instance_table_refs)
                || expr_references_running_apps(right, instance_table_refs)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_running_apps(expr, instance_table_refs)
                || expr_references_running_apps(low, instance_table_refs)
                || expr_references_running_apps(high, instance_table_refs)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_running_apps(expr, instance_table_refs)
                || list
                    .iter()
                    .any(|expr| expr_references_running_apps(expr, instance_table_refs))
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            expr_references_running_apps(expr, instance_table_refs)
                || expr_references_running_apps(pattern, instance_table_refs)
        }
        Expr::Function(function) => {
            function_arguments_reference_running_apps(&function.parameters, instance_table_refs)
                || function_arguments_reference_running_apps(&function.args, instance_table_refs)
                || function
                    .filter
                    .as_ref()
                    .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
                || function
                    .within_group
                    .iter()
                    .any(|expr| expr_references_running_apps(&expr.expr, instance_table_refs))
                || function.over.as_ref().is_some_and(|over| match over {
                    WindowType::WindowSpec(spec) => {
                        spec.partition_by
                            .iter()
                            .any(|expr| expr_references_running_apps(expr, instance_table_refs))
                            || spec.order_by.iter().any(|expr| {
                                expr_references_running_apps(&expr.expr, instance_table_refs)
                            })
                    }
                    WindowType::NamedWindow(_) => false,
                })
        }
        Expr::Cast { expr, .. }
        | Expr::AtTimeZone {
            timestamp: expr, ..
        }
        | Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Prefixed { value: expr, .. } => {
            expr_references_running_apps(expr, instance_table_refs)
        }
        Expr::Position { expr, r#in } => {
            expr_references_running_apps(expr, instance_table_refs)
                || expr_references_running_apps(r#in, instance_table_refs)
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            expr_references_running_apps(expr, instance_table_refs)
                || substring_from
                    .as_ref()
                    .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
                || substring_for
                    .as_ref()
                    .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            expr_references_running_apps(expr, instance_table_refs)
                || trim_what
                    .as_ref()
                    .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
                || trim_characters.as_ref().is_some_and(|exprs| {
                    exprs
                        .iter()
                        .any(|expr| expr_references_running_apps(expr, instance_table_refs))
                })
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
            ..
        } => {
            expr_references_running_apps(expr, instance_table_refs)
                || expr_references_running_apps(overlay_what, instance_table_refs)
                || expr_references_running_apps(overlay_from, instance_table_refs)
                || overlay_for
                    .as_ref()
                    .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
                || conditions.iter().any(|when| {
                    expr_references_running_apps(&when.condition, instance_table_refs)
                        || expr_references_running_apps(&when.result, instance_table_refs)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_references_running_apps(expr, instance_table_refs))
        }
        Expr::Tuple(exprs) => exprs
            .iter()
            .any(|expr| expr_references_running_apps(expr, instance_table_refs)),
        _ => false,
    }
}

fn function_arguments_reference_running_apps(
    args: &FunctionArguments,
    instance_table_refs: &HashSet<String>,
) -> bool {
    match args {
        FunctionArguments::None => false,
        FunctionArguments::Subquery(_) => false,
        FunctionArguments::List(args) => {
            args.args
                .iter()
                .any(|arg| function_arg_references_running_apps(arg, instance_table_refs))
                || args.clauses.iter().any(|clause| match clause {
                    FunctionArgumentClause::OrderBy(exprs) => exprs
                        .iter()
                        .any(|expr| expr_references_running_apps(&expr.expr, instance_table_refs)),
                    FunctionArgumentClause::Limit(expr) => {
                        expr_references_running_apps(expr, instance_table_refs)
                    }
                    FunctionArgumentClause::Having(having) => {
                        expr_references_running_apps(&having.1, instance_table_refs)
                    }
                    FunctionArgumentClause::IgnoreOrRespectNulls(_)
                    | FunctionArgumentClause::OnOverflow(_)
                    | FunctionArgumentClause::Separator(_)
                    | FunctionArgumentClause::JsonNullClause(_) => false,
                })
        }
    }
}

fn function_arg_references_running_apps(
    arg: &FunctionArg,
    instance_table_refs: &HashSet<String>,
) -> bool {
    match arg {
        FunctionArg::Named { arg, .. } => {
            function_arg_expr_references_running_apps(arg, instance_table_refs)
        }
        FunctionArg::ExprNamed { name, arg, .. } => {
            expr_references_running_apps(name, instance_table_refs)
                || function_arg_expr_references_running_apps(arg, instance_table_refs)
        }
        FunctionArg::Unnamed(arg) => {
            function_arg_expr_references_running_apps(arg, instance_table_refs)
        }
    }
}

fn function_arg_expr_references_running_apps(
    arg: &FunctionArgExpr,
    instance_table_refs: &HashSet<String>,
) -> bool {
    match arg {
        FunctionArgExpr::Expr(expr) => expr_references_running_apps(expr, instance_table_refs),
        FunctionArgExpr::QualifiedWildcard(name) => {
            object_name_matches_reference(name, instance_table_refs)
        }
        FunctionArgExpr::Wildcard => false,
    }
}

fn compound_identifier_references_running_apps(
    idents: &[sqlparser::ast::Ident],
    instance_table_refs: &HashSet<String>,
) -> bool {
    let Some(column) = idents.last() else {
        return false;
    };
    if !sql_identifier_eq(&column.value, RUNNING_APPS_COLUMN_NAME) {
        return false;
    }
    if idents.len() == 1 || instance_table_refs.is_empty() {
        return true;
    }
    idents[..idents.len() - 1]
        .iter()
        .any(|ident| instance_table_refs.contains(&normalize_sql_identifier(&ident.value)))
}

fn object_name_matches_reference(name: &ObjectName, references: &HashSet<String>) -> bool {
    name.0.iter().any(|part| {
        part.as_ident()
            .is_some_and(|ident| references.contains(&normalize_sql_identifier(&ident.value)))
    })
}

fn object_name_matches(name: &ObjectName, expected: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| sql_identifier_eq(&ident.value, expected))
}

fn sql_identifier_eq(actual: &str, expected: &str) -> bool {
    normalize_sql_identifier(actual) == normalize_sql_identifier(expected)
}

fn normalize_sql_identifier(value: &str) -> String {
    value.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use crate::actions::discovery::tests::FakeCoordinator;
    use crate::actions::query::{
        collect_query_table_requirements, extract_tables, SessionContextExtend,
    };

    #[test]
    fn test_extract_table_names() {
        let sql = "select * from instances";
        let tables = extract_tables(sql);
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&"instances".to_string()));
    }

    #[test]
    fn test_instances_query_without_running_apps_skips_app_count() {
        let requirements = collect_query_table_requirements(
            "select ip, status from instances where status = 'ACTIVE'",
        );
        assert_eq!(requirements.required_tables, vec!["instances".to_string()]);
        assert!(!requirements.include_instance_running_apps);
    }

    #[test]
    fn test_instances_query_with_running_apps_loads_app_count() {
        let requirements = collect_query_table_requirements(
            "select ip from instances where running_apps > 0 order by running_apps desc",
        );
        assert!(requirements.include_instance_running_apps);
    }

    #[test]
    fn test_instances_order_by_qualified_running_apps_loads_app_count() {
        let requirements = collect_query_table_requirements(
            "select i.ip from instances i order by i.running_apps",
        );
        assert!(requirements.include_instance_running_apps);
    }

    #[test]
    fn test_instances_join_on_running_apps_loads_app_count() {
        let requirements = collect_query_table_requirements(
            "select i.ip from instances i join active_apps a on i.running_apps > 0",
        );
        assert!(requirements.include_instance_running_apps);
    }

    #[test]
    fn test_instances_aggregate_running_apps_loads_app_count() {
        let requirements =
            collect_query_table_requirements("select max(running_apps) from instances");
        assert!(requirements.include_instance_running_apps);
    }

    #[test]
    fn test_instances_wildcard_loads_app_count() {
        let requirements = collect_query_table_requirements("select * from instances");
        assert!(requirements.include_instance_running_apps);
    }

    #[test]
    fn test_instances_qualified_wildcard_loads_app_count() {
        let requirements = collect_query_table_requirements(
            "select i.* from instances as i where i.status = 'ACTIVE'",
        );
        assert!(requirements.include_instance_running_apps);
    }

    #[test]
    fn test_unrelated_qualified_wildcard_skips_app_count() {
        let requirements = collect_query_table_requirements(
            "select a.* from active_apps a join instances i on a.app_id <> i.ip",
        );
        assert!(!requirements.include_instance_running_apps);
    }

    #[tokio::test]
    #[ignore]
    async fn test_connect_with_real_coordinator() {
        let coordinator_url = "http://xxx:21001";
        let context = SessionContextExtend::new(coordinator_url, None)
            .await
            .unwrap();
        let df = context.sql("select * from active_apps").await.unwrap();
        df.show().await.unwrap();
    }

    #[tokio::test]
    async fn test_query_json_output_with_empty_result() {
        use crate::actions::query::{OutputFormat, QueryAction};
        use crate::actions::Action;

        let port = riffle_server::util::find_available_port().unwrap();
        let _coordinator =
            crate::actions::discovery::tests::FakeCoordinator::new(port as i32).await;
        let coordinator_url = format!("http://localhost:{}", port);

        let action = QueryAction::new(
            "select * from instances where ip = 'no-such-host'".to_string(),
            OutputFormat::Json,
            coordinator_url,
        );
        action.act().await.unwrap();
    }

    #[tokio::test]
    async fn test_register_table_instances() {
        let port = riffle_server::util::find_available_port().unwrap();
        let coordinator = FakeCoordinator::new(port as i32).await;
        let coordinator_url = format!("http://localhost:{}", port);

        let context = SessionContextExtend::new(&coordinator_url, None)
            .await
            .unwrap();

        let sql = "SELECT * FROM instances";
        let df = context.sql(sql).await.unwrap();
        // df.show().await.unwrap();

        context
            .sql("select * from instances where grpc_port > -1")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();

        assert_eq!(1, df.count().await.unwrap());
    }
}
