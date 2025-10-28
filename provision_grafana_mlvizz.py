#!/usr/bin/env python3
# Grafana 11 compatible MLVizz dashboard provisioning
# (One-time) install the Python dependency: python3 -m pip install --upgrade requests
# Author: Joel Antony - Data Engineer Intern - Simplyai

import os, json, requests

# -------------------------------
# Environment variable configuration with defaults
# -------------------------------
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000").rstrip("/")  # Grafana base URL
GRAFANA_USER = os.environ.get("GRAFANA_USER", "admin")  # Grafana username
GRAFANA_PASS = os.environ.get("GRAFANA_PASS", "admin")  # Grafana password
DS_NAME = os.environ.get("DS_NAME", "Postgres Insurance (internal)")  # Data source name
PG_HOST = os.environ.get("PG_HOST", "postgres:5432")  # PostgreSQL host:port
PG_DB = os.environ.get("PG_DB", "insurance")  # Database name
PG_USER = os.environ.get("PG_USER", "postgres")  # Database user
PG_PASS = os.environ.get("PG_PASS", "postgres")  # Database password
FOLDER_TITLE = "MLVizz"  # Grafana folder to store the dashboard
DASHBOARD_TITLE = "MLVizz Insurance Overview"  # Dashboard title

# -------------------------------
# Create a reusable HTTP session for Grafana API calls
# -------------------------------
s = requests.Session()
s.auth = (GRAFANA_USER, GRAFANA_PASS)
s.headers.update({"Content-Type": "application/json"})

# Helper functions for GET and POST requests to Grafana REST API
def gget(p):
    """Perform GET request to Grafana API and handle errors."""
    r = s.get(f"{GRAFANA_URL}{p}")
    r.raise_for_status()
    return r

def gpost(p, d):
    """Perform POST request to Grafana API and handle errors."""
    r = s.post(f"{GRAFANA_URL}{p}", data=json.dumps(d))
    r.raise_for_status()
    return r

# -------------------------------
# Ensure PostgreSQL datasource exists or create it
# -------------------------------
def ensure_ds():
    try:
        # Try fetching existing datasource by name
        r = gget(f"/api/datasources/name/{DS_NAME}")
        ds = r.json()
        print("[i] DS exists", ds.get("uid"))
        return ds["uid"]
    except:
        pass  # Datasource not found, proceed to create

    # Create new PostgreSQL datasource configuration
    d = {
        "name": DS_NAME,
        "type": "postgres",
        "access": "proxy",
        "url": PG_HOST,
        "user": PG_USER,
        "database": PG_DB,
        "basicAuth": False,
        "isDefault": False,
        "jsonData": {"sslmode": "disable"},
        "secureJsonData": {"password": PG_PASS},
    }

    # Post request to create datasource
    ds = gpost("/api/datasources", d).json()["datasource"]
    print("[+] DS created", ds.get("uid"))
    return ds["uid"]

# -------------------------------
# Ensure folder exists or create it
# -------------------------------
def ensure_folder():
    # Fetch all existing folders and check if the desired one exists
    for f in gget("/api/folders").json():
        if f["title"] == FOLDER_TITLE:
            return f["id"]
    # If not found, create new folder
    return gpost("/api/folders", {"title": FOLDER_TITLE}).json()["id"]

# -------------------------------
# Build the dashboard JSON structure dynamically
# -------------------------------
def build(ds_uid):
    # Define dashboard variables (template filters)
    vars = [
        {"name": "insurancetype", "label": "Insurance Type", "type": "query",
         "datasource": {"type": "postgres", "uid": ds_uid},
         "definition": "SELECT DISTINCT insurancetype AS __text, insurancetype AS __value FROM policies ORDER BY insurancetype;",
         "query": "SELECT DISTINCT insurancetype AS __text, insurancetype AS __value FROM policies ORDER BY insurancetype;",
         "includeAll": True, "multi": True},
        {"name": "status", "label": "Status", "type": "query",
         "datasource": {"type": "postgres", "uid": ds_uid},
         "definition": "SELECT DISTINCT status AS __text, status AS __value FROM claims ORDER BY status;",
         "query": "SELECT DISTINCT status AS __text, status AS __value FROM claims ORDER BY status;",
         "includeAll": True, "multi": True},
        {"name": "agentid", "label": "Agent", "type": "query",
         "datasource": {"type": "postgres", "uid": ds_uid},
         "definition": "SELECT agentname AS __text, agentid AS __value FROM agents ORDER BY agentname;",
         "query": "SELECT agentname AS __text, agentid AS __value FROM agents ORDER BY agentname;",
         "includeAll": True, "multi": True}
    ]

    # Helper functions for defining Grafana panels
    def stat(title, q, x, y):
        """Create a 'Stat' panel (single value metric)."""
        return {"type": "stat", "title": title, "datasource": {"type": "postgres", "uid": ds_uid},
                "gridPos": {"h": 6, "w": 8, "x": x, "y": y},
                "targets": [{"refId": "A", "rawSql": q, "format": "table"}]}

    def bar(title, q, x, y):
        """Create a 'Bar chart' panel."""
        return {"type": "barchart", "title": title, "datasource": {"type": "postgres", "uid": ds_uid},
                "gridPos": {"h": 10, "w": 12, "x": x, "y": y},
                "targets": [{"refId": "A", "rawSql": q, "format": "table"}]}

    def ts(title, q, x, y):
        """Create a 'Time series' panel."""
        return {"type": "timeseries", "title": title, "datasource": {"type": "postgres", "uid": ds_uid},
                "gridPos": {"h": 10, "w": 24, "x": x, "y": y},
                "targets": [{"refId": "A", "rawSql": q, "format": "time_series"}]}

    # -------------------------------
    # SQL queries for each panel metric
    # -------------------------------
    tp = """SELECT COUNT(*) AS total_count FROM policies
            WHERE (ARRAY['ALL']&&ARRAY[${insurancetype}] OR insurancetype=ANY(ARRAY[${insurancetype}]))
            AND (ARRAY['ALL']&&ARRAY[${agentid}] OR policies.agentid::text=ANY(ARRAY[${agentid}]));"""

    tc = """SELECT COUNT(*) AS total_count FROM claims
            WHERE (ARRAY['ALL']&&ARRAY[${status}] OR status=ANY(ARRAY[${status}]))
            AND $__timeFilter(claimdate);"""

    ti = "SELECT COUNT(*) AS total_count FROM insured_items;"

    pbt = """SELECT insurancetype AS label,COUNT(*) AS count FROM policies
             WHERE (ARRAY['ALL']&&ARRAY[${insurancetype}] OR insurancetype=ANY(ARRAY[${insurancetype}]))
             AND (ARRAY['ALL']&&ARRAY[${agentid}] OR policies.agentid::text=ANY(ARRAY[${agentid}]))
             GROUP BY insurancetype ORDER BY count DESC;"""

    cbs = """SELECT status AS label,COUNT(*) AS count FROM claims
             WHERE (ARRAY['ALL']&&ARRAY[${status}] OR status=ANY(ARRAY[${status}]))
             AND $__timeFilter(claimdate)
             GROUP BY status ORDER BY count DESC;"""

    cot = """SELECT DATE_TRUNC('month',claimdate) AS time,COUNT(*) AS count FROM claims
             WHERE (ARRAY['ALL']&&ARRAY[${status}] OR status=ANY(ARRAY[${status}]))
             AND $__timeFilter(claimdate)
             GROUP BY 1 ORDER BY 1;"""

    # Arrange panels on the dashboard grid
    panels = [
        stat("Total Policies", tp, 0, 0),
        stat("Total Claims", tc, 8, 0),
        stat("Total Insured Items", ti, 16, 0),
        bar("Policies by Insurance Type", pbt, 0, 6),
        bar("Claims by Status", cbs, 12, 6),
        ts("Claims Over Time", cot, 0, 16)
    ]

    # Return complete dashboard JSON definition
    return {"id": None, "uid": None, "title": DASHBOARD_TITLE, "editable": True,
            "timezone": "browser", "templating": {"list": vars},
            "time": {"from": "now-3y", "to": "now"}, "refresh": "1m", "panels": panels}

# -------------------------------
# Create or update dashboard in Grafana
# -------------------------------
def upsert(fid, d):
    # Push dashboard JSON to Grafana API (creates or updates existing)
    r = gpost("/api/dashboards/db",
              {"dashboard": d, "folderId": fid, "overwrite": True, "message": "v2 update"}).json()
    print("[+] Dashboard created", r.get("url"))

# -------------------------------
# Script entry point
# -------------------------------
if __name__ == "__main__":
    print("[i] Checking Grafana health")
    print(gget("/api/health").json())  # Verify Grafana server is reachable

    uid = ensure_ds()        # Ensure data source exists and get its UID
    fid = ensure_folder()    # Ensure folder exists and get its ID
    dash = build(uid)        # Build dashboard structure
    upsert(fid, dash)        # Create/update dashboard in Grafana

    # Final success message
    print("[✓] Open Grafana → Dashboards → MLVizz → MLVizz Insurance Overview")