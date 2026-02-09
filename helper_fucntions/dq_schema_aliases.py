# helper_functions/dq_schema_aliases.py

COLUMN_ALIASES = {
    # campaign level
    "campaign_name": [
        "campaign_name",
        "campaign",
        "campaign_nm",
        "cm_campaign_name",
    ],
    # adset / ad group / line item / insertion order
    "adset_name": [
        "adset_name",
        "ad_set_name",
        "adgroup_name",
        "ad_group_name",
        "ad_group",
        "line_item_name",
        "insertion_order_name",
        "io_name",
        "media_buy_name",
        "placement_group_name",
    ],
    # creative
    "creative_name": [
        "creative_name",
        "ad_name",
        "ad",
        "creative",
        "asset_name",
    ],
    # placement (sometimes DV360/CM360 have both)
    "placement_name": [
        "placement_name",
        "cm_placement_name",
        "site_placement",
        "inventory_source",
    ],
    # tracking
    "utm_campaign": ["utm_campaign", "ga_campaign", "campaign_code"],
    "utm_source": ["utm_source", "ga_source"],
    "utm_medium": ["utm_medium", "ga_medium"],
}
