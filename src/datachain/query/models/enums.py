from enum import Enum


class Sorting(Enum):
    """Sort direction for ORDER BY clauses"""
    ASC = "ASC"
    DESC = "DESC"


class Comparator(str, Enum):
    """Comparison operators for filtering data"""
    LESS_THAN = "<"
    GREATER_THAN = ">"
    EQUAL = "="
    NOT_EQUAL = "!="
    LESS_THAN_OR_EQUAL = "<="
    GREATER_THAN_OR_EQUAL = ">="
    IN = "IN"
    NOT_IN = "NOT IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"



class Aggregation(str, Enum):
    """Aggregation functions for metric calculations"""
    SUM = "SUM"
    AVG = "AVG"
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    MIN = "MIN"
    MAX = "MAX"
    MEDIAN = "MEDIAN"