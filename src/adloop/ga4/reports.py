"""GA4 report tools — account summaries and custom reports."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from adloop.config import AdLoopConfig


def get_account_summaries(config: AdLoopConfig) -> dict:
    """List all GA4 accounts and properties the authenticated user can access.

    Returns both a flat ``properties`` list (best for quickly picking a
    property_id) and a nested ``accounts`` structure.  The ``property_id``
    field in each entry is the bare numeric ID ready to pass directly to any
    other GA4 tool.
    """
    from adloop.ga4.client import get_admin_client

    client = get_admin_client(config)
    summaries = client.list_account_summaries()

    accounts = []
    flat_properties = []

    for summary in summaries:
        account_id = summary.account.replace("accounts/", "")
        properties = []
        for prop in summary.property_summaries:
            # Resource name is "properties/123456789" — strip the prefix so
            # the value can be passed directly as property_id to other tools.
            numeric_id = prop.property.replace("properties/", "")
            entry = {
                "property_id": numeric_id,
                "property_name": prop.display_name,
                "account_id": account_id,
                "account_name": summary.display_name,
            }
            properties.append(entry)
            flat_properties.append(entry)

        accounts.append({
            "account_id": account_id,
            "account_name": summary.display_name,
            "properties": properties,
        })

    return {
        # Flat list — easiest for the LLM to scan and pick a property_id.
        "properties": flat_properties,
        # Nested by account — useful when the user wants to see client grouping.
        "accounts": accounts,
        "total_accounts": len(accounts),
        "total_properties": len(flat_properties),
    }


def run_ga4_report(
    config: AdLoopConfig,
    *,
    property_id: str = "",
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    date_range_start: str = "7daysAgo",
    date_range_end: str = "today",
    limit: int = 100,
) -> dict:
    """Run a GA4 report with specified dimensions, metrics, and date range."""
    from google.analytics.data_v1beta.types import (
        DateRange,
        Dimension,
        Metric,
        RunReportRequest,
    )

    from adloop.ga4.client import get_data_client

    if not dimensions and not metrics:
        return {"error": "At least one dimension or metric must be specified."}

    client = get_data_client(config)

    request = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name=d) for d in (dimensions or [])],
        metrics=[Metric(name=m) for m in (metrics or [])],
        date_ranges=[DateRange(start_date=date_range_start, end_date=date_range_end)],
        limit=limit,
    )

    response = client.run_report(request)

    dim_headers = [h.name for h in response.dimension_headers]
    met_headers = [h.name for h in response.metric_headers]

    rows = []
    for row in response.rows:
        r = {}
        for i, val in enumerate(row.dimension_values):
            r[dim_headers[i]] = val.value
        for i, val in enumerate(row.metric_values):
            r[met_headers[i]] = val.value
        rows.append(r)

    return {
        "property": property_id,
        "date_range": {"start": date_range_start, "end": date_range_end},
        "dimensions": dim_headers,
        "metrics": met_headers,
        "rows": rows,
        "row_count": len(rows),
        "total_row_count": response.row_count,
    }


def run_realtime_report(
    config: AdLoopConfig,
    *,
    property_id: str = "",
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
) -> dict:
    """Run a GA4 realtime report."""
    from google.analytics.data_v1beta.types import (
        Dimension,
        Metric,
        RunRealtimeReportRequest,
    )

    from adloop.ga4.client import get_data_client

    client = get_data_client(config)

    request = RunRealtimeReportRequest(
        property=property_id,
        dimensions=[Dimension(name=d) for d in (dimensions or [])],
        metrics=[Metric(name=m) for m in (metrics or ["activeUsers"])],
    )

    response = client.run_realtime_report(request)

    dim_headers = [h.name for h in response.dimension_headers]
    met_headers = [h.name for h in response.metric_headers]

    rows = []
    for row in response.rows:
        r = {}
        for i, val in enumerate(row.dimension_values):
            r[dim_headers[i]] = val.value
        for i, val in enumerate(row.metric_values):
            r[met_headers[i]] = val.value
        rows.append(r)

    return {
        "property": property_id,
        "dimensions": dim_headers,
        "metrics": met_headers,
        "rows": rows,
        "row_count": len(rows),
    }
