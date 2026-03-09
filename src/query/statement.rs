#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlStatement {
    SelectCount {
        table_name: String,
    },
    SelectColumns {
        table_name: String,
        column_names: Vec<String>,
        where_clause: Option<Disjunction>,
        order_by: Vec<OrderByTerm>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Disjunction {
    pub arms: Vec<Conjunction>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conjunction {
    pub terms: Vec<WhereTerm>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WhereTerm {
    pub column_name: String,
    pub op: WhereOperator,
    pub value: QueryValue,
    pub second_value: Option<QueryValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhereOperator {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
    Between,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryValue {
    Text(String),
    Integer(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderByTerm {
    pub column_name: String,
    pub direction: SortDirection,
}
