"""Google Ads read tools — campaign, ad, keyword, and search term performance."""
# test premier commit 

from __future__ import annotations
from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from adloop.config import AdLoopConfig


def list_accounts(config: AdLoopConfig) -> dict:
    """List all accessible Google Ads accounts."""
    from adloop.ads.gaql import execute_query

    mcc_id = config.ads.login_customer_id
    if mcc_id:
        query = """
            SELECT customer_client.id, customer_client.descriptive_name,
                   customer_client.status, customer_client.manager
            FROM customer_client
        """
        rows = execute_query(config, mcc_id, query)
    else:
        query = """
            SELECT customer.id, customer.descriptive_name,
                   customer.status, customer.manager
            FROM customer
            LIMIT 1
        """
        rows = execute_query(config, config.ads.customer_id, query)

    return {"accounts": rows, "total_accounts": len(rows)}

def list_campaigns(config: AdLoopConfig, customer_id, campaign_status_filter, with_spend) -> dict:
    """List all accessible Google Ads campaigns for a specific customer id."""
    from adloop.ads.gaql import execute_query
    campaign_filter = _campaign_status_filter(campaign_status_filter)
    spend_filter = _spend_filter(with_spend)

    query = f"""
        SELECT campaign.id, campaign.name, campaign.status
        FROM campaign
        WHERE campaign.status != 'REMOVED'
        {campaign_filter}
        {spend_filter}
    """
    rows = execute_query(config, customer_id, query)
   
    return {"campaigns": rows, "total_campaigns": len(rows)}

def list_ad_groups(config: AdLoopConfig, customer_id, campaign_status_filter, with_spend, campaign_ids,ad_group_status_filter) -> dict:
    """List all accessible Google Ads ad groups for a specific customer id and for specific campaigns."""
    from adloop.ads.gaql import execute_query

    campaign_filter_status = _campaign_status_filter(campaign_status_filter)
    spend_filter = _spend_filter(with_spend)
    campaign_filter_ids = _campaign_ids_filter(campaign_ids)

    ad_group_filter_status = _ad_group_status_filter(ad_group_status_filter)


    query = f"""
        SELECT campaign.id, campaign.name, campaign.status,
        ad_group.id, ad_group.name, ad_group.status
        FROM ad_group
        WHERE campaign.status != 'REMOVED'
        {campaign_filter_status}
        {campaign_filter_ids}
        {ad_group_filter_status}
        {spend_filter}
    """
    rows = execute_query(config, customer_id, query)
   
    return {"ad_groups": rows, "total_ad_groups": len(rows)}

def get_campaign_performance(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    date_range_start: str = "",
    date_range_end: str = "",
    campaign_ids:list = [""],
    campaign_status_filter:str = 'ENABLED',
    with_spend:bool = True
) -> dict:
    """Get campaign-level performance metrics for the given date range."""
    from adloop.ads.gaql import execute_query    

    date_clause = _date_clause(date_range_start, date_range_end)
    spend_filter = _spend_filter(with_spend)
    campaign_filter_ids = _campaign_ids_filter(campaign_ids)
    campaign_filter_status = _campaign_status_filter(campaign_status_filter)
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status,
            campaign.advertising_channel_type, campaign.bidding_strategy_type,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.conversions_value,
            metrics.ctr, metrics.average_cpc, metrics.search_impression_share,
            metrics.search_top_impression_share, metrics.search_absolute_top_impression_share, 
            metrics.search_rank_lost_impression_share
        FROM campaign
        WHERE campaign.status != 'REMOVED'
        {campaign_filter_status}
        {campaign_filter_ids}
        {date_clause}
        {spend_filter}
        ORDER BY metrics.cost_micros DESC
    """

    if customer_id == "4560674777": # Erie construction
        query_conversion_action = f"""
        SELECT 
        campaign.id, campaign.name,
            segments.conversion_action_name,
            metrics.all_conversions,
            metrics.all_conversions_value
        FROM campaign
        WHERE campaign.status != 'REMOVED'
        AND segments.conversion_action_name IN (
          '[Roofing] Workable Lead',
          '[Roofing] Net Sale',
          '[Roofing] Lead',
          '[Roofing] Appointment Set',
          '[Roofing] Issues',
          '[Roofing] Hits',
          '[Roofing] confirmations',
          '(Kashurba) Get Pricing'
          )
        AND metrics.all_conversions > 0
        {campaign_filter_status}
        {campaign_filter_ids}
        {date_clause}
"""     

    rows = execute_query(config, customer_id, query)
    _enrich_cost_fields(rows)

    if customer_id == "4560674777":
        rows_conversion_action = execute_query(config, customer_id, query_conversion_action)
        rows = _aggregate(rows,rows_conversion_action)

    return {"campaigns": rows, "total_campaigns": len(rows)}


def get_ad_group_performance(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    date_range_start: str = "",
    date_range_end: str = "",
    campaign_ids:list = [""],
    campaign_status_filter:str = 'ENABLED',
    ad_group_ids:list = [""],
    ad_group_status_filter:str = 'ENABLED',
    with_spend:bool = True
) -> dict:
    """Get ad group-level performance metrics for the given date range."""
    from adloop.ads.gaql import execute_query

    date_clause = _date_clause(date_range_start, date_range_end)
    campaign_filter_ids = _campaign_ids_filter(campaign_ids)
    campaign_filter_status = _campaign_status_filter(campaign_status_filter)
    ad_group_filter_ids = _ad_group_ids_filter(ad_group_ids)
    ad_group_filter_status = _ad_group_status_filter(ad_group_status_filter)
    spend_filter = _spend_filter(with_spend)

    query = f"""
        SELECT campaign.id, campaign.name,
               ad_group.id, ad_group.name, ad_group.status, ad_group.type,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.conversions_value,
               metrics.ctr, metrics.average_cpc
        FROM ad_group
        WHERE ad_group.status != 'REMOVED'
        {spend_filter}
        {date_clause}
        {campaign_filter_status}
        {campaign_filter_ids}
        {ad_group_filter_ids}
        {ad_group_filter_status}
        ORDER BY metrics.cost_micros DESC
    """

    rows = execute_query(config, customer_id, query)
    _enrich_cost_fields(rows)

    return {"ad_groups": rows, "total_ad_groups": len(rows)}


def get_asset_group_performance(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    date_range_start: str = "",
    date_range_end: str = "",
    campaign_ids:list = [""],
    campaign_status_filter:str = 'ENABLED',
    asset_group_ids:list = [""],
    asset_group_status_filter:str = 'ENABLED',
    with_spend:bool = True
) -> dict:
    """Get asset group-level performance for Performance Max campaigns."""
    from adloop.ads.gaql import execute_query

    date_clause = _date_clause(date_range_start, date_range_end)
    campaign_filter_ids = _campaign_ids_filter(campaign_ids)
    campaign_filter_status = _campaign_status_filter(campaign_status_filter)
    asset_group_filter_ids = _asset_group_ids_filter(asset_group_ids)
    asset_group_filter_status = _asset_group_status_filter(asset_group_status_filter)
    spend_filter = _spend_filter(with_spend)

    query = f"""
        SELECT campaign.id, campaign.name,
               asset_group.id, asset_group.name, asset_group.status,
               asset_group.ad_strength,
               asset_group.final_urls,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.conversions_value, metrics.ctr
        FROM asset_group
        WHERE asset_group.status != 'REMOVED'
        {spend_filter}
        {date_clause}
        {campaign_filter_status}
        {campaign_filter_ids}
        {asset_group_filter_ids}
        {asset_group_filter_status}
        ORDER BY metrics.cost_micros DESC
    """

    rows = execute_query(config, customer_id, query)
    _enrich_cost_fields(rows)

    return {"asset_groups": rows, "total_asset_groups": len(rows)}


def get_asset_group_asset_performance(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    date_range_start: str = "",
    date_range_end: str = "",
    campaign_ids:list = [""],
    campaign_status_filter:str = 'ENABLED',
    asset_group_asset_ids:list = [""],
    asset_group_asset_status_filter:str = 'ENABLED',
    with_spend:bool = True
) -> dict:
    """Get per-asset performance for Performance Max campaigns via asset_group_asset.

    Returns metrics broken down by individual asset within a PMax asset group,
    along with performance_label (BEST | GOOD | LOW | PENDING | UNSPECIFIED)
    and primary_status for each asset.
    """
    from adloop.ads.gaql import execute_query

    date_clause = _date_clause(date_range_start, date_range_end)
    campaign_filter_ids = _campaign_ids_filter(campaign_ids)
    campaign_filter_status = _campaign_status_filter(campaign_status_filter)
    asset_group_asset_filter_ids = _asset_group_asset_ids_filter(asset_group_asset_ids)
    asset_group_asset_filter_status = _asset_group_asset_status_filter(asset_group_asset_status_filter)
    spend_filter = _spend_filter(with_spend)

    query = f"""
        SELECT campaign.id, campaign.name,
               asset_group.id, asset_group.name,
               asset_group_asset.field_type,
               asset_group_asset.performance_label,
               asset_group_asset.primary_status,
               asset_group_asset.status,
               asset.id, asset.name, asset.type,
               asset.text_asset.text,
               asset.image_asset.full_size.url,
               asset.youtube_video_asset.youtube_video_id,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.conversions_value, metrics.ctr
        FROM asset_group_asset
        WHERE asset_group.status != 'REMOVED'
        {spend_filter}
        {date_clause}
        {campaign_filter_status}
        {campaign_filter_ids}
        {asset_group_asset_filter_ids}
        {asset_group_asset_filter_status}
        ORDER BY metrics.impressions DESC
    """

    rows = execute_query(config, customer_id, query)
    _enrich_cost_fields(rows)

    return {"assets": rows, "total_assets": len(rows)}


def get_ad_group_ad_asset_performance(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    date_range_start: str = "",
    date_range_end: str = "",
    campaign_ids:list = [""],
    campaign_status_filter:str = 'ENABLED',
    asset_group_ad_asset_ids:list = [""],
    asset_group_ad_asset_status_filter:str = 'ENABLED',
    with_spend:bool = True
) -> dict:
    """Get per-asset performance for RSA ads via ad_group_ad_asset_view.

    Returns actual metrics (impressions, clicks, cost) broken down by individual
    asset, along with the performance label (BEST | GOOD | LOW | PENDING).
    Works for Responsive Search Ads and other ad types that use assets.
    """
    from adloop.ads.gaql import execute_query

    date_clause = _date_clause(date_range_start, date_range_end)
    campaign_filter_ids = _campaign_ids_filter(campaign_ids)
    campaign_filter_status = _campaign_status_filter(campaign_status_filter)
    asset_group_ad_asset_filter_ids = _asset_group_ad_asset_ids_filter(asset_group_ad_asset_ids)
    asset_group_ad_asset_filter_status = _asset_group_ad_asset_status_filter(asset_group_ad_asset_status_filter)
    spend_filter = _spend_filter(with_spend)

    query = f"""
        SELECT campaign.id, campaign.name,
               ad_group.id, ad_group.name,
               ad_group_ad.ad.id,
               asset.id, asset.name, asset.type,
               asset.text_asset.text,
               asset.image_asset.full_size.url,
               asset.youtube_video_asset.youtube_video_id,
               ad_group_ad_asset_view.field_type,
               ad_group_ad_asset_view.performance_label,
               ad_group_ad_asset_view.enabled,
               ad_group_ad_asset_view.pinned_field,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.ctr
        FROM ad_group_ad_asset_view
        WHERE asset_group.status != 'REMOVED'
        {spend_filter}
        {date_clause}
        {campaign_filter_status}
        {campaign_filter_ids}
        {asset_group_ad_asset_filter_ids}
        {asset_group_ad_asset_filter_status}
        ORDER BY metrics.impressions DESC
    """

    rows = execute_query(config, customer_id, query)
    _enrich_cost_fields(rows)

    return {"assets": rows, "total_assets": len(rows)}


def get_ad_performance(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    date_range_start: str = "",
    date_range_end: str = "",
    campaign_ids:list = [""],
    campaign_status_filter:str = 'ENABLED',
    ad_group_ids:list = [""],
    ad_group_status_filter:str = 'ENABLED',
    ad_group_ad_ids:list = [""],
    ad_group_ad_status_filter:str = 'ENABLED',
    with_spend:bool = True
) -> dict:
    """Get ad-level performance data including headlines, descriptions, and metrics."""
    from adloop.ads.gaql import execute_query

    date_clause = _date_clause(date_range_start, date_range_end)
    campaign_filter_ids = _campaign_ids_filter(campaign_ids)
    campaign_filter_status = _campaign_status_filter(campaign_status_filter)
    ad_group_filter_ids = _ad_group_ids_filter(ad_group_ids)
    ad_group_filter_status = _ad_group_status_filter(ad_group_status_filter)
    ad_group_ad_filter_ids = _ad_group_ids_filter(ad_group_ad_ids)
    ad_group_ad_filter_status = _ad_group_status_filter(ad_group_ad_status_filter)
    spend_filter = _spend_filter(with_spend)

    query = f"""
        SELECT campaign.name, ad_group.name,
               ad_group_ad.ad.id, ad_group_ad.ad.type,
               ad_group_ad.ad.responsive_search_ad.headlines,
               ad_group_ad.ad.responsive_search_ad.descriptions,
               ad_group_ad.ad.final_urls,
               ad_group_ad.status,
               metrics.impressions, metrics.clicks, metrics.ctr,
               metrics.conversions, metrics.cost_micros
        FROM ad_group_ad
        WHERE asset_group.status != 'REMOVED'
        {spend_filter}
        {date_clause}
        {campaign_filter_status}
        {campaign_filter_ids}
        {ad_group_filter_ids}
        {ad_group_filter_status}
        {ad_group_ad_filter_ids}
        {ad_group_ad_filter_status}
        ORDER BY metrics.cost_micros DESC
    """

    rows = execute_query(config, customer_id, query)
    _enrich_cost_fields(rows)

    return {"ads": rows, "total_ads": len(rows)}


def get_keyword_performance(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    date_range_start: str = "",
    date_range_end: str = "",
) -> dict:
    """Get keyword metrics including quality scores and competitive data."""
    from adloop.ads.gaql import execute_query

    date_clause = _date_clause(date_range_start, date_range_end)

    query = f"""
        SELECT campaign.name, ad_group.name,
               ad_group_criterion.keyword.text,
               ad_group_criterion.keyword.match_type,
               ad_group_criterion.quality_info.quality_score,
               metrics.impressions, metrics.clicks, metrics.ctr,
               metrics.average_cpc, metrics.cost_micros,
               metrics.conversions
        FROM keyword_view
        WHERE ad_group_criterion.status != 'REMOVED'
          AND metrics.cost_micros > 0
          {date_clause}
        ORDER BY metrics.cost_micros DESC
    """

    rows = execute_query(config, customer_id, query)
    _enrich_cost_fields(rows)

    return {"keywords": rows, "total_keywords": len(rows)}


def get_search_terms(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    date_range_start: str = "",
    date_range_end: str = "",
) -> dict:
    """Get search terms report — what users actually typed before clicking ads."""
    from adloop.ads.gaql import execute_query

    if date_range_start and date_range_end:
        query = f"""
            SELECT search_term_view.search_term,
                   campaign.name, ad_group.name,
                   metrics.impressions, metrics.clicks,
                   metrics.cost_micros, metrics.conversions
            FROM search_term_view
            WHERE metrics.cost_micros > 0
              AND segments.date BETWEEN '{date_range_start}' AND '{date_range_end}'
            ORDER BY metrics.clicks DESC
            LIMIT 200
        """
    else:
        query = """
            SELECT search_term_view.search_term,
                   campaign.name, ad_group.name,
                   metrics.impressions, metrics.clicks,
                   metrics.cost_micros, metrics.conversions
            FROM search_term_view
            WHERE metrics.cost_micros > 0
              AND segments.date DURING LAST_30_DAYS
            ORDER BY metrics.clicks DESC
            LIMIT 200
        """

    rows = execute_query(config, customer_id, query)
    _enrich_cost_fields(rows)

    return {"search_terms": rows, "total_search_terms": len(rows)}


def get_product_performance(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    date_range_start: str = "",
    date_range_end: str = "",
) -> dict:
    """Get product-level performance via shopping_performance_view."""
    from adloop.ads.gaql import execute_query

    date_clause = _date_clause(date_range_start, date_range_end)

    query = f"""
        SELECT campaign.id, campaign.name,
               ad_group.id, ad_group.name,
               segments.product_item_id,
               segments.product_title,
               segments.product_brand,
               segments.product_type_l1,
               segments.product_category_level1,
               segments.product_condition,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.conversions_value, metrics.ctr
        FROM shopping_performance_view
        WHERE metrics.cost_micros > 0
          {date_clause}
        ORDER BY metrics.cost_micros DESC
        LIMIT 500
    """

    rows = execute_query(config, customer_id, query)
    _enrich_cost_fields(rows)

    return {"products": rows, "total_products": len(rows)}


def get_negative_keywords(
    config: AdLoopConfig,
    *,
    customer_id: str = "",
    campaign_id: str = "",
) -> dict:
    """List negative keywords for a campaign or all campaigns."""
    from adloop.ads.gaql import execute_query

    campaign_filter = ""
    if campaign_id:
        campaign_filter = f"AND campaign.id = {campaign_id}"

    query = f"""
        SELECT campaign.id, campaign.name,
               campaign_criterion.keyword.text,
               campaign_criterion.keyword.match_type,
               campaign_criterion.negative,
               campaign_criterion.criterion_id
        FROM campaign_criterion
        WHERE campaign_criterion.negative = TRUE
          AND campaign_criterion.status != 'REMOVED'
          {campaign_filter}
        ORDER BY campaign.name
    """

    rows = execute_query(config, customer_id, query)
    return {"negative_keywords": rows, "total_negative_keywords": len(rows)}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _date_clause(start: str, end: str) -> str:
    """Build a GAQL date WHERE fragment."""
    if start and end:
        return f"AND segments.date BETWEEN '{start}' AND '{end}'"
    return "AND segments.date DURING LAST_30_DAYS"

def _spend_filter(with_spend):
    filter = ""
    if with_spend :
        filter = "AND metrics.cost_micros > 0"
    return filter

def _campaign_ids_filter(campaign_ids):
    filter = ""
    if campaign_ids != [""] :
        ids = ", ".join(map(str, campaign_ids))
        filter = f"AND campaign.id IN ({ids})"
    return filter

def _campaign_status_filter(campaign_status_filter):
    filter = ""
    if campaign_status_filter:
        filter = f"AND campaign.status = '{campaign_status_filter}'"
    return filter

def _ad_group_ids_filter(ad_group_ids):
    filter = ""
    if ad_group_ids != [""] :
        ids = ", ".join(map(str, ad_group_ids))
        filter = f"AND ad_group.id IN ({ids})"
    return filter

def _ad_group_status_filter(ad_group_status_filter):
    filter = ""
    if ad_group_status_filter:
        filter = f"AND ad_group.status = '{ad_group_status_filter}'"
    return filter

def _asset_group_ids_filter(asset_group_ids):
    filter = ""
    if asset_group_ids != [""] :
        ids = ", ".join(map(str, asset_group_ids))
        filter = f"AND asset_group.id IN ({ids})"
    return filter

def _asset_group_status_filter(asset_group_status_filter):
    filter = ""
    if asset_group_status_filter:
        filter = f"AND asset_group.status = '{asset_group_status_filter}'"
    return filter

def _asset_group_asset_ids_filter(asset_group_asset_ids):
    filter = ""
    if asset_group_asset_ids != [""] :
        ids = ", ".join(map(str, asset_group_asset_ids))
        filter = f"AND asset_group_asset.id IN ({ids})"
    return filter

def _asset_group_asset_status_filter(asset_group_asset_status_filter):
    filter = ""
    if asset_group_asset_status_filter:
        filter = f"AND asset_group_asset.status = '{asset_group_asset_status_filter}'"
    return filter

def _asset_group_ad_asset_ids_filter(asset_group_ad_asset_ids):
    filter = ""
    if asset_group_ad_asset_ids != [""] :
        ids = ", ".join(map(str, asset_group_ad_asset_ids))
        filter = f"AND asset_group_ad_asset.id IN ({ids})"
    return filter

def _asset_group_ad_asset_status_filter(asset_group_ad_asset_status_filter):
    filter = ""
    if asset_group_ad_asset_status_filter:
        filter = f"AND asset_group_ad_asset.status = '{asset_group_ad_asset_status_filter}'"
    return filter


def _enrich_cost_fields(rows: list[dict]) -> None:
    """Add human-readable cost and CPA fields computed from cost_micros."""
    for row in rows:
        cost_micros = row.get("metrics.cost_micros", 0) or 0
        row["metrics.cost"] = round(cost_micros / 1_000_000, 2)

        conversions = row.get("metrics.conversions", 0) or 0
        if conversions > 0:
            row["metrics.cpa"] = round(cost_micros / 1_000_000 / conversions, 2)

        avg_cpc_micros = row.get("metrics.average_cpc", 0) or 0
        if avg_cpc_micros:
            row["metrics.average_cpc_eur"] = round(avg_cpc_micros / 1_000_000, 2)

def _aggregate(rows, rows_conversion_action):
    # dictionary of keys : campaign ids ; values: associated KPIs
    campaign_map = {r['campaign.id']: r for r in rows}

    for r_conv in rows_conversion_action:
        campaign_id = r_conv['campaign.id']
        
        if campaign_id not in campaign_map:
            continue
        
        campaign = campaign_map[campaign_id]
        action_name = r_conv['segments.conversion_action_name']

        conv = r_conv.get('metrics.all_conversions', 0) or 0
        value = r_conv.get('metrics.all_conversions_value', 0) or 0

        # Initialization if the colums doesn't exists yet
        campaign.setdefault(action_name, 0)
        campaign.setdefault(f"{action_name} - value", 0)

        # aggregation (sum)
        campaign[action_name] += conv
        campaign[f"{action_name} - value"] += value

    return list(campaign_map.values())

