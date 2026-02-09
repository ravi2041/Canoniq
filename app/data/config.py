
# Platform-to-table mapping (only needed if you want to query individually by platform)
PLATFORM_TABLE_MAP = {
    "facebook": "facebook_data",
    "tiktok": "tiktok_data",
    "youtube": "youtube_data",
    "cm360_conversion": "cm360_conversion_data"
}

# Column names (these should match your SQL SELECTs)
DATE_COL       = "date"
PLATFORM_COL   = "site"        # now coming directly from DB
CAMPAIGN_COL   = "campaign"
CREATIVE_COL   = "creative"
PLACEMENT_COL  = "placement"
ACTIVITY_COL   = "activity_name"

# Conversions table (still useful if you query it separately)
CM360_TABLE        = "cm360_conversion_data"
CM_DATE_COL        = "date"
CM_CAMPAIGN_COL    = "campaign"
CM_ACTIVITY_NAME   = "activity"
CM_CONVERSIONS_COL = "total_conversions"
