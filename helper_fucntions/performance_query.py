"""
Performance query module for marketing data aggregation.
Now includes CTR, CVR, CPC, CPA, and cost-per-impression metrics.
MySQL-safe with NULLIF() for divide-by-zero protection.
"""

query = """
WITH facebook_agg AS (
    SELECT
        `date`,
        advertiser,
        campaign,
        site,
        placement,
        creative,
        'NA' AS activity_name,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        0 AS conversions
    FROM facebook_data
    GROUP BY `date`, advertiser, campaign, site, placement, creative, activity_name
),

tiktok_agg AS (
    SELECT
        `date`,
        advertiser,
        campaign,
        site,
        placement,
        creative,
        'NA' AS activity_name,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        0 AS conversions
    FROM tiktok_data
    GROUP BY `date`, advertiser, campaign, site, placement, creative, activity_name
),

youtube_agg AS (
    SELECT
        `date`,
        advertiser,
        campaign,
        site,
        placement,
        creative,
        'NA' AS activity_name,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        0 AS conversions
    FROM youtube_data
    GROUP BY `date`, advertiser, campaign, site, placement, creative, activity_name
),

cm360_conv_agg AS (
    SELECT
        `date`,
        advertiser,
        campaign,
        site,
        placement,
        creative,
        activity AS activity_name,
        0 AS impressions,
        0 AS clicks,
        0 AS cost,
        SUM(total_conversions) AS conversions
    FROM cm360_conversion_data
    GROUP BY `date`, advertiser, campaign, site, placement, creative, activity_name
),

combined AS (
    SELECT * FROM facebook_agg
    UNION ALL
    SELECT * FROM tiktok_agg
    UNION ALL
    SELECT * FROM youtube_agg
)

SELECT
    c.`date`,
    c.advertiser,
    c.campaign,
    c.site as platform,
    c.placement,
    c.creative,
    conv.activity_name,

    -- Base delivery metrics
    COALESCE(c.impressions, 0) AS impressions,
    COALESCE(c.clicks, 0) AS clicks,
    COALESCE(c.cost, 0) AS cost,
    COALESCE(conv.conversions, 0) AS conversions,

    -- Derived performance metrics (MySQL-safe)
    COALESCE(c.clicks / NULLIF(c.impressions, 0), 0) AS ctr,       -- Click Through Rate
    COALESCE(conv.conversions / NULLIF(c.clicks, 0), 0) AS cvr,     -- Conversion Rate
    COALESCE(c.cost / NULLIF(c.clicks, 0), 0) AS cpc,               -- Cost Per Click
    COALESCE(c.cost / NULLIF(conv.conversions, 0), 0) AS cpa,       -- Cost Per Acquisition
    COALESCE(c.cost / NULLIF(c.impressions, 0), 0) AS cost_per_impression

FROM combined c
LEFT JOIN cm360_conv_agg conv
    ON c.`date` = conv.`date`
   AND c.campaign = conv.campaign
   AND c.site = conv.site
   AND c.placement = conv.placement
   AND c.creative = conv.creative

LIMIT 1000;
"""
