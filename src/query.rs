use anyhow::{Result, bail};
use sqlparser::ast::{
    Expr, Fetch, FunctionArg, FunctionArgExpr, FunctionArguments, LockClause, Select, SelectItem,
    SetExpr, Statement, TableFactor, Top, TopQuantity,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::{Parser, ParserError};

use crate::error::SqliteParseError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlStatement {
    SelectCount {
        table_name: String,
    },
    SelectColumns {
        table_name: String,
        column_names: Vec<String>,
    },
}

impl SqlStatement {
    pub fn parse(sql: &str) -> Result<Self> {
        let dialect = SQLiteDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).map_err(map_parser_error)?;

        if statements.len() != 1 {
            bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
        }

        let statement = statements.pop().expect("single statement must exist");
        Self::parse_statement(statement, sql)
    }

    fn parse_statement(statement: Statement, sql: &str) -> Result<Self> {
        let Statement::Query(query) = statement else {
            bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
        };

        Self::parse_select_query(*query, sql)
    }

    fn parse_select_query(query: sqlparser::ast::Query, sql: &str) -> Result<Self> {
        validate_query(&query, sql)?;

        let SetExpr::Select(select) = *query.body else {
            bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
        };
        let select = *select;

        validate_select(&select, sql)?;

        let table_name = parse_table_name(&select.from[0].relation, &select.from[0].joins, sql)?;
        if let [SelectItem::UnnamedExpr(Expr::Function(function))] = select.projection.as_slice()
            && is_count_star(function)
        {
            return Ok(Self::SelectCount { table_name });
        }

        let column_names = select
            .projection
            .iter()
            .map(parse_projection_column)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| SqliteParseError::UnsupportedSql(sql.to_owned()))?;

        Ok(Self::SelectColumns {
            table_name,
            column_names,
        })
    }
}

fn validate_query(query: &sqlparser::ast::Query, sql: &str) -> Result<()> {
    if let Some(fetch) = &query.fetch {
        return Err(fetch_syntax_error(fetch).into());
    }
    if let Some(format_clause) = &query.format_clause {
        return Err(SqliteParseError::SqlSyntaxErrorNear(
            first_token(&format_clause.to_string()).unwrap_or_else(|| "FORMAT".to_owned()),
        )
        .into());
    }
    if query.settings.is_some() {
        return Err(SqliteParseError::SqlSyntaxErrorNear("SETTINGS".to_owned()).into());
    }
    if !query.locks.is_empty() {
        return Err(lock_syntax_error(&query.locks[0]).into());
    }
    if !query.pipe_operators.is_empty() {
        return Err(SqliteParseError::SqlSyntaxError.into());
    }

    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.for_clause.is_some()
    {
        bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
    }

    Ok(())
}

fn validate_select(select: &Select, sql: &str) -> Result<()> {
    if let Some(top) = &select.top {
        return Err(top_syntax_error(top).into());
    }
    if let Some(qualify) = &select.qualify {
        return Err(expr_syntax_error(qualify).into());
    }
    if select.select_modifiers.is_some()
        || select.exclude.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || select.value_table_mode.is_some()
    {
        bail!(SqliteParseError::SqlSyntaxError);
    }

    if select.distinct.is_some()
        || select.into.is_some()
        || select.selection.is_some()
        || !select.group_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.from.len() != 1
        || select.projection.is_empty()
    {
        bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
    }

    Ok(())
}

fn parse_table_name(
    relation: &TableFactor,
    joins: &[sqlparser::ast::Join],
    sql: &str,
) -> Result<String> {
    if !joins.is_empty() {
        bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
    }

    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = relation
    else {
        bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
    };

    if alias.is_some()
        || args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
    }

    let Some(first_part) = name.0.first() else {
        bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
    };
    if name.0.len() != 1 {
        bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
    }

    Ok(first_part.to_string())
}

fn is_count_star(function: &sqlparser::ast::Function) -> bool {
    function.name.to_string().eq_ignore_ascii_case("count")
        && matches!(function.parameters, FunctionArguments::None)
        && matches!(
            &function.args,
            FunctionArguments::List(arguments)
                if arguments.duplicate_treatment.is_none()
                    && arguments.clauses.is_empty()
                    && matches!(
                        arguments.args.as_slice(),
                        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                    )
        )
        && function.filter.is_none()
        && function.null_treatment.is_none()
        && function.over.is_none()
        && function.within_group.is_empty()
}

fn parse_projection_column(select_item: &SelectItem) -> Option<String> {
    match select_item {
        SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => Some(identifier.value.clone()),
        _ => None,
    }
}

fn map_parser_error(error: ParserError) -> anyhow::Error {
    let message = error.to_string();
    let message = message
        .strip_prefix("sql parser error: ")
        .unwrap_or(&message);

    if message.contains("found: EOF") {
        return SqliteParseError::SqlIncompleteInput.into();
    }

    if let Some(token) = message.split("found: ").nth(1).and_then(first_token) {
        return SqliteParseError::SqlSyntaxErrorNear(token).into();
    }

    SqliteParseError::SqlSyntaxError.into()
}

fn top_syntax_error(top: &Top) -> SqliteParseError {
    let token = match &top.quantity {
        Some(TopQuantity::Constant(quantity)) => quantity.to_string(),
        Some(TopQuantity::Expr(expr)) => {
            first_token(&expr.to_string()).unwrap_or_else(|| "TOP".to_owned())
        }
        None => "TOP".to_owned(),
    };
    SqliteParseError::SqlSyntaxErrorNear(token)
}

fn fetch_syntax_error(fetch: &Fetch) -> SqliteParseError {
    let token = if fetch.quantity.is_some() {
        "FIRST".to_owned()
    } else {
        "FETCH".to_owned()
    };
    SqliteParseError::SqlSyntaxErrorNear(token)
}

fn lock_syntax_error(lock_clause: &LockClause) -> SqliteParseError {
    SqliteParseError::SqlSyntaxErrorNear(lock_clause.lock_type.to_string())
}

fn expr_syntax_error(expr: &Expr) -> SqliteParseError {
    SqliteParseError::SqlSyntaxErrorNear(
        first_token(&expr.to_string()).unwrap_or_else(|| "syntax".to_owned()),
    )
}

fn first_token(value: &str) -> Option<String> {
    value
        .split_ascii_whitespace()
        .next()
        .map(|token| {
            token
                .trim_matches(|ch: char| ch == '"' || ch == '\'' || ch == '`' || ch == ',')
                .to_owned()
        })
        .filter(|token| !token.is_empty())
}

trait GroupByExt {
    fn is_empty(&self) -> bool;
}

impl GroupByExt for sqlparser::ast::GroupByExpr {
    fn is_empty(&self) -> bool {
        match self {
            sqlparser::ast::GroupByExpr::All(_) => false,
            sqlparser::ast::GroupByExpr::Expressions(expressions, _) => expressions.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SqliteParseError;

    #[test]
    fn parses_count_query() {
        assert_eq!(
            SqlStatement::parse("SELECT COUNT(*) FROM apples").unwrap(),
            SqlStatement::SelectCount {
                table_name: "apples".to_owned(),
            }
        );
    }

    #[test]
    fn parses_single_column_query() {
        assert_eq!(
            SqlStatement::parse("SELECT name FROM apples").unwrap(),
            SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["name".to_owned()],
            }
        );
    }

    #[test]
    fn parses_multi_column_query() {
        assert_eq!(
            SqlStatement::parse("SELECT name, color FROM apples").unwrap(),
            SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["name".to_owned(), "color".to_owned()],
            }
        );
    }

    #[test]
    fn rejects_where_clause_as_unsupported_feature() {
        let error =
            SqlStatement::parse("SELECT name FROM apples WHERE color = 'green'").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedSql(sql)
                if sql == "SELECT name FROM apples WHERE color = 'green'"
        ));
    }

    #[test]
    fn rejects_multiple_columns_as_unsupported_feature() {
        let error = SqlStatement::parse("SELECT name AS n FROM apples").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedSql(sql) if sql == "SELECT name AS n FROM apples"
        ));
    }

    #[test]
    fn rejects_expression_projection_as_unsupported_feature() {
        let error = SqlStatement::parse("SELECT upper(name) FROM apples").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedSql(sql) if sql == "SELECT upper(name) FROM apples"
        ));
    }

    #[test]
    fn mirrors_incomplete_input() {
        let error = SqlStatement::parse("SELECT name FROM apples WHERE").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(error, SqliteParseError::SqlIncompleteInput));
    }

    #[test]
    fn mirrors_syntax_error_for_missing_projection() {
        let error = SqlStatement::parse("SELECT FROM apples").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::SqlSyntaxErrorNear(token) if token == "FROM"
        ));
    }

    #[test]
    fn mirrors_syntax_error_for_top() {
        let error = SqlStatement::parse("SELECT TOP 1 name FROM apples").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::SqlSyntaxErrorNear(token) if token == "1"
        ));
    }
}
