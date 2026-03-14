"""Shared hierarchy filter builder — used by all routers."""
from typing import Optional


def hierarchy_filter(
    division: Optional[str] = None,
    customer: Optional[str] = None,
    platform: Optional[str] = None,
    table_alias: str = "",
) -> tuple[str, list]:
    """Build optional WHERE clause fragments for division/customer/platform.

    Returns (sql_fragment, params_list).
    sql_fragment starts with " AND ..." if any filters active, or "" if none.
    """
    prefix = f"{table_alias}." if table_alias else ""
    clauses = []
    params = []
    if division:
        clauses.append(f"{prefix}division = ?")
        params.append(division)
    if customer:
        clauses.append(f"{prefix}customer = ?")
        params.append(customer)
    if platform:
        clauses.append(f"{prefix}platform = ?")
        params.append(platform)
    sql = (" AND " + " AND ".join(clauses)) if clauses else ""
    return sql, params
