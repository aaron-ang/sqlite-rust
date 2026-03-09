use crate::query::QueryValue;
use crate::storage::record::RecordValue;

/// Evaluate a lower bound predicate (>, >=) against a decoded record value.
pub fn record_satisfies_lower(
    actual: &RecordValue<'_>,
    bound: &QueryValue,
    inclusive: bool,
) -> bool {
    match (actual, bound) {
        (RecordValue::Text(actual), QueryValue::Text(min)) => {
            if inclusive {
                *actual >= min.as_str()
            } else {
                *actual > min.as_str()
            }
        }
        (RecordValue::Integer(actual), QueryValue::Integer(min)) => {
            if inclusive {
                *actual >= *min
            } else {
                *actual > *min
            }
        }
        _ => false,
    }
}

/// Evaluate an upper bound predicate (<, <=) against a decoded record value.
pub fn record_satisfies_upper(
    actual: &RecordValue<'_>,
    bound: &QueryValue,
    inclusive: bool,
) -> bool {
    match (actual, bound) {
        (RecordValue::Text(actual), QueryValue::Text(max)) => {
            if inclusive {
                *actual <= max.as_str()
            } else {
                *actual < max.as_str()
            }
        }
        (RecordValue::Integer(actual), QueryValue::Integer(max)) => {
            if inclusive {
                *actual <= *max
            } else {
                *actual < *max
            }
        }
        _ => false,
    }
}
