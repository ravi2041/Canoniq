# diag_ga4.py
import os, json
from google.oauth2 import service_account
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from dotenv import load_dotenv
load_dotenv()

PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")  # should be digits, e.g. "345678901"
key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
assert PROPERTY_ID and key_path, "Set GA4_PROPERTY_ID and GOOGLE_APPLICATION_CREDENTIALS"

creds = service_account.Credentials.from_service_account_file(key_path)
print("Service account email:", creds.service_account_email)
print("Using property:", PROPERTY_ID)

client = BetaAnalyticsDataClient(credentials=creds)

try:
    req = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date="2024-01-01", end_date="2024-01-07")],
        limit=1,
    )
    resp = client.run_report(req)
    print("OK ✅ Rows:", len(resp.rows))
except Exception as e:
    print("Failed ❌:", repr(e))
    print("Likely causes:")
    print("- Wrong Property ID (must be numeric, not G- or account id)")
    print("- Service account email not added in GA4 Property Access Management")
    print("- Data API not enabled on the service account’s Cloud project")
