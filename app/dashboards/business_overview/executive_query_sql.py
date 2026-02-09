query = """
    SELECT
        `date`,
        advertiser,
        campaign,
        site,
        placement,
        creative,
        activity_name,
        impressions,
        clicks,
        cost,
        conversions
    FROM business_overview_snapshot
"""
