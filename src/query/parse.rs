use anyhow::{Result, bail};
use sqlparser::ast::{
    BinaryOperator, Expr, Fetch, FunctionArg, FunctionArgExpr, FunctionArguments, LockClause,
    OrderBy, OrderByExpr, OrderByKind, Select, SelectItem, SetExpr, Statement, TableFactor, Top,
    Value,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::{Parser, ParserError};

use crate::error::SqliteParseError;

use super::statement::{
    Conjunction, Disjunction, OrderByTerm, QueryValue, SortDirection, SqlStatement, WhereTerm,
};

type ParseResult<T> = std::result::Result<T, QueryBuildError>;

#[derive(Debug)]
enum QueryBuildError {
    Unsupported,
    Sqlite(SqliteParseError),
}

impl From<SqliteParseError> for QueryBuildError {
    fn from(error: SqliteParseError) -> Self {
        Self::Sqlite(error)
    }
}

impl SqlStatement {
    pub fn parse(sql: &str) -> Result<Self> {
        let dialect = SQLiteDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).map_err(map_parser_error)?;

        if statements.len() != 1 {
            bail!(SqliteParseError::UnsupportedSql(sql.to_owned()));
        }

        let statement = statements.pop().expect("single statement must exist");
        Self::parse_statement(statement).map_err(|error| match error {
            QueryBuildError::Unsupported => SqliteParseError::UnsupportedSql(sql.to_owned()).into(),
            QueryBuildError::Sqlite(error) => error.into(),
        })
    }

    fn parse_statement(statement: Statement) -> ParseResult<Self> {
        let Statement::Query(query) = statement else {
            return Err(QueryBuildError::Unsupported);
        };

        Self::parse_select_query(*query)
    }

    fn parse_select_query(query: sqlparser::ast::Query) -> ParseResult<Self> {
        validate_query(&query)?;

        let SetExpr::Select(select) = *query.body else {
            return Err(QueryBuildError::Unsupported);
        };
        let select = *select;

        validate_select(&select)?;

        let table_name = parse_table_name(&select.from[0].relation, &select.from[0].joins)?;
        let where_clause = Disjunction::parse_expr_opt(select.selection.as_ref())?;
        let order_by = OrderByTerm::parse_many(query.order_by.as_ref())?;

        if let [SelectItem::UnnamedExpr(Expr::Function(function))] = select.projection.as_slice()
            && is_count_star(function)
            && where_clause.is_none()
            && order_by.is_empty()
        {
            return Ok(Self::SelectCount { table_name });
        }

        let column_names = select
            .projection
            .iter()
            .map(parse_projection_column)
            .collect::<Option<Vec<_>>>()
            .ok_or(QueryBuildError::Unsupported)?;

        Ok(Self::SelectColumns {
            table_name,
            column_names,
            where_clause,
            order_by,
        })
    }
}

impl Disjunction {
    fn parse_expr_opt(selection: Option<&Expr>) -> ParseResult<Option<Self>> {
        selection.map(Self::parse_expr).transpose()
    }

    fn parse_expr(expr: &Expr) -> ParseResult<Self> {
        match strip_nested(expr) {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Or => {
                let mut arms = Self::parse_expr(left)?.arms;
                arms.extend(Self::parse_expr(right)?.arms);
                Ok(Self { arms })
            }
            expr => Ok(Self {
                arms: vec![Conjunction::parse_expr(expr)?],
            }),
        }
    }
}

impl Conjunction {
    fn parse_expr(expr: &Expr) -> ParseResult<Self> {
        match strip_nested(expr) {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
                let mut terms = Self::parse_expr(left)?.terms;
                terms.extend(Self::parse_expr(right)?.terms);
                Ok(Self { terms })
            }
            expr => Ok(Self {
                terms: vec![WhereTerm::parse_expr(expr)?],
            }),
        }
    }
}

impl WhereTerm {
    fn parse_expr(expr: &Expr) -> ParseResult<Self> {
        let Expr::BinaryOp { left, op, right } = strip_nested(expr) else {
            return Err(QueryBuildError::Unsupported);
        };

        if *op != BinaryOperator::Eq {
            return Err(QueryBuildError::Unsupported);
        }

        let Expr::Identifier(identifier) = strip_nested(left.as_ref()) else {
            return Err(QueryBuildError::Unsupported);
        };

        let value = QueryValue::parse_expr(strip_nested(right.as_ref()))?;

        Ok(Self {
            column_name: identifier.value.clone(),
            value,
        })
    }
}

impl QueryValue {
    fn parse_expr(expr: &Expr) -> ParseResult<Self> {
        let Expr::Value(value) = expr else {
            return Err(QueryBuildError::Unsupported);
        };

        match &value.value {
            Value::SingleQuotedString(text) => Ok(Self::Text(text.clone())),
            Value::Number(number, false) => number
                .parse::<i64>()
                .map(Self::Integer)
                .map_err(|_| QueryBuildError::Unsupported),
            _ => Err(QueryBuildError::Unsupported),
        }
    }
}

impl OrderByTerm {
    fn parse_many(order_by: Option<&OrderBy>) -> ParseResult<Vec<Self>> {
        let Some(order_by) = order_by else {
            return Ok(Vec::new());
        };

        if order_by.interpolate.is_some() {
            return Err(QueryBuildError::Unsupported);
        }

        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Err(QueryBuildError::Unsupported);
        };

        expressions.iter().map(Self::parse_expr).collect()
    }

    fn parse_expr(order_by_expr: &OrderByExpr) -> ParseResult<Self> {
        if order_by_expr.with_fill.is_some() || order_by_expr.options.nulls_first.is_some() {
            return Err(QueryBuildError::Unsupported);
        }

        let Expr::Identifier(identifier) = strip_nested(&order_by_expr.expr) else {
            return Err(QueryBuildError::Unsupported);
        };

        let direction = match order_by_expr.options.asc {
            Some(false) => SortDirection::Desc,
            _ => SortDirection::Asc,
        };

        Ok(Self {
            column_name: identifier.value.clone(),
            direction,
        })
    }
}

fn validate_query(query: &sqlparser::ast::Query) -> ParseResult<()> {
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

    if query.with.is_some() || query.limit_clause.is_some() || query.for_clause.is_some() {
        return Err(QueryBuildError::Unsupported);
    }

    Ok(())
}

fn validate_select(select: &Select) -> ParseResult<()> {
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
        return Err(SqliteParseError::SqlSyntaxError.into());
    }

    if select.distinct.is_some()
        || select.into.is_some()
        || !group_by_is_empty(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.from.len() != 1
        || select.projection.is_empty()
    {
        return Err(QueryBuildError::Unsupported);
    }

    Ok(())
}

fn parse_table_name(relation: &TableFactor, joins: &[sqlparser::ast::Join]) -> ParseResult<String> {
    if !joins.is_empty() {
        return Err(QueryBuildError::Unsupported);
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
        return Err(QueryBuildError::Unsupported);
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
        return Err(QueryBuildError::Unsupported);
    }

    let Some(first_part) = name.0.first() else {
        return Err(QueryBuildError::Unsupported);
    };
    if name.0.len() != 1 {
        return Err(QueryBuildError::Unsupported);
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

fn strip_nested(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(expr) => strip_nested(expr),
        _ => expr,
    }
}

fn map_parser_error(error: ParserError) -> anyhow::Error {
    match error {
        ParserError::RecursionLimitExceeded => SqliteParseError::SqlSyntaxError.into(),
        ParserError::TokenizerError(message) => map_parser_message(&message),
        ParserError::ParserError(message) => map_parser_message(&message),
    }
}

fn map_parser_message(message: &str) -> anyhow::Error {
    if message.contains("found: EOF") {
        return SqliteParseError::SqlIncompleteInput.into();
    }

    if let Some(token) = syntax_error_token(message) {
        return SqliteParseError::SqlSyntaxErrorNear(token).into();
    }

    SqliteParseError::SqlSyntaxError.into()
}

fn syntax_error_token(message: &str) -> Option<String> {
    if let Some(token) = message
        .strip_prefix("Expected: an expression, found: ")
        .and_then(first_token)
    {
        return Some(token);
    }

    if let Some(token) = message
        .strip_prefix("Expected: identifier, found: ")
        .and_then(first_token)
    {
        return Some(token);
    }

    if let Some(token) = message
        .strip_prefix("Expected: SELECT item, found: ")
        .and_then(first_token)
    {
        return Some(token);
    }

    None
}

fn fetch_syntax_error(fetch: &Fetch) -> SqliteParseError {
    SqliteParseError::SqlSyntaxErrorNear(first_token(&fetch.to_string()).unwrap_or_else(|| {
        if fetch.percent {
            "PERCENT".to_owned()
        } else {
            "FETCH".to_owned()
        }
    }))
}

fn lock_syntax_error(lock_clause: &LockClause) -> SqliteParseError {
    SqliteParseError::SqlSyntaxErrorNear(
        first_token(&lock_clause.to_string()).unwrap_or_else(|| "FOR".to_owned()),
    )
}

fn top_syntax_error(top: &Top) -> SqliteParseError {
    let token = match top.quantity.as_ref() {
        Some(sqlparser::ast::TopQuantity::Expr(expr)) => {
            first_token(&expr.to_string()).unwrap_or_else(|| "TOP".to_owned())
        }
        Some(sqlparser::ast::TopQuantity::Constant(quantity)) => quantity.to_string(),
        None => "TOP".to_owned(),
    };

    SqliteParseError::SqlSyntaxErrorNear(token)
}

fn expr_syntax_error(expr: &Expr) -> SqliteParseError {
    SqliteParseError::SqlSyntaxErrorNear(first_token(&expr.to_string()).unwrap_or_default())
}

fn first_token(input: &str) -> Option<String> {
    input.split_whitespace().next().map(ToOwned::to_owned)
}

fn group_by_is_empty(group_by: &sqlparser::ast::GroupByExpr) -> bool {
    matches!(
        group_by,
        sqlparser::ast::GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty()
    )
}

#[cfg(test)]
mod tests {
    use sqlparser::parser::ParserError;

    use super::*;

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
                where_clause: None,
                order_by: vec![],
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
                where_clause: None,
                order_by: vec![],
            }
        );
    }

    #[test]
    fn parses_where_conjunction() {
        assert_eq!(
            SqlStatement::parse("SELECT id, name FROM apples WHERE color = 'Yellow' AND id = 4")
                .unwrap(),
            SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["id".to_owned(), "name".to_owned()],
                where_clause: Some(Disjunction {
                    arms: vec![Conjunction {
                        terms: vec![
                            WhereTerm {
                                column_name: "color".to_owned(),
                                value: QueryValue::Text("Yellow".to_owned()),
                            },
                            WhereTerm {
                                column_name: "id".to_owned(),
                                value: QueryValue::Integer(4),
                            },
                        ],
                    }],
                }),
                order_by: vec![],
            }
        );
    }

    #[test]
    fn parses_top_level_or_where_clause() {
        assert_eq!(
            SqlStatement::parse(
                "SELECT id, name FROM apples WHERE color = 'Yellow' OR color = 'Red'"
            )
            .unwrap(),
            SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["id".to_owned(), "name".to_owned()],
                where_clause: Some(Disjunction {
                    arms: vec![
                        Conjunction {
                            terms: vec![WhereTerm {
                                column_name: "color".to_owned(),
                                value: QueryValue::Text("Yellow".to_owned()),
                            }],
                        },
                        Conjunction {
                            terms: vec![WhereTerm {
                                column_name: "color".to_owned(),
                                value: QueryValue::Text("Red".to_owned()),
                            }],
                        },
                    ],
                }),
                order_by: vec![],
            }
        );
    }

    #[test]
    fn parses_mixed_or_of_ands() {
        assert_eq!(
            SqlStatement::parse(
                "SELECT id, name FROM apples WHERE color = 'Yellow' AND id = 4 OR color = 'Red'"
            )
            .unwrap(),
            SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["id".to_owned(), "name".to_owned()],
                where_clause: Some(Disjunction {
                    arms: vec![
                        Conjunction {
                            terms: vec![
                                WhereTerm {
                                    column_name: "color".to_owned(),
                                    value: QueryValue::Text("Yellow".to_owned()),
                                },
                                WhereTerm {
                                    column_name: "id".to_owned(),
                                    value: QueryValue::Integer(4),
                                },
                            ],
                        },
                        Conjunction {
                            terms: vec![WhereTerm {
                                column_name: "color".to_owned(),
                                value: QueryValue::Text("Red".to_owned()),
                            }],
                        },
                    ],
                }),
                order_by: vec![],
            }
        );
    }

    #[test]
    fn parses_order_by_clause() {
        assert_eq!(
            SqlStatement::parse("SELECT id, name FROM apples ORDER BY name").unwrap(),
            SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["id".to_owned(), "name".to_owned()],
                where_clause: None,
                order_by: vec![OrderByTerm {
                    column_name: "name".to_owned(),
                    direction: SortDirection::Asc,
                }],
            }
        );
    }

    #[test]
    fn parses_multi_column_desc_order_by_clause() {
        assert_eq!(
            SqlStatement::parse("SELECT id, name FROM apples ORDER BY color DESC, name DESC")
                .unwrap(),
            SqlStatement::SelectColumns {
                table_name: "apples".to_owned(),
                column_names: vec!["id".to_owned(), "name".to_owned()],
                where_clause: None,
                order_by: vec![
                    OrderByTerm {
                        column_name: "color".to_owned(),
                        direction: SortDirection::Desc,
                    },
                    OrderByTerm {
                        column_name: "name".to_owned(),
                        direction: SortDirection::Desc,
                    },
                ],
            }
        );
    }

    #[test]
    fn rejects_unsupported_where_operator() {
        let error =
            SqlStatement::parse("SELECT name FROM apples WHERE color != 'green'").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedSql(sql)
                if sql == "SELECT name FROM apples WHERE color != 'green'"
        ));
    }

    #[test]
    fn rejects_alias_projection_as_unsupported_feature() {
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
    fn rejects_function_in_where_clause() {
        let error = SqlStatement::parse("SELECT name FROM apples WHERE upper(color) = 'YELLOW'")
            .unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedSql(sql)
                if sql == "SELECT name FROM apples WHERE upper(color) = 'YELLOW'"
        ));
    }

    #[test]
    fn rejects_invalid_order_by_expression() {
        let error =
            SqlStatement::parse("SELECT name FROM apples ORDER BY upper(name)").unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedSql(sql)
                if sql == "SELECT name FROM apples ORDER BY upper(name)"
        ));
    }

    #[test]
    fn rejects_unsupported_boolean_shape() {
        let error = SqlStatement::parse(
            "SELECT name FROM apples WHERE (color = 'Yellow' OR color = 'Red') AND id = 4",
        )
        .unwrap_err();
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::UnsupportedSql(sql)
                if sql == "SELECT name FROM apples WHERE (color = 'Yellow' OR color = 'Red') AND id = 4"
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

        assert!(matches!(error, SqliteParseError::SqlSyntaxError));
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

    #[test]
    fn maps_parser_error_eof_to_incomplete_input() {
        let error = map_parser_error(ParserError::ParserError(
            "Expected: an expression, found: EOF".to_owned(),
        ));
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(error, SqliteParseError::SqlIncompleteInput));
    }

    #[test]
    fn maps_parser_error_identifier_to_near_token() {
        let error = map_parser_error(ParserError::ParserError(
            "Expected: identifier, found: FROM".to_owned(),
        ));
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(
            error,
            SqliteParseError::SqlSyntaxErrorNear(token) if token == "FROM"
        ));
    }

    #[test]
    fn maps_tokenizer_error_to_syntax_error() {
        let error = map_parser_error(ParserError::TokenizerError("bad token".to_owned()));
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(error, SqliteParseError::SqlSyntaxError));
    }

    #[test]
    fn maps_recursion_limit_to_syntax_error() {
        let error = map_parser_error(ParserError::RecursionLimitExceeded);
        let error = error
            .downcast_ref::<SqliteParseError>()
            .expect("error should downcast to SqliteParseError");

        assert!(matches!(error, SqliteParseError::SqlSyntaxError));
    }
}
