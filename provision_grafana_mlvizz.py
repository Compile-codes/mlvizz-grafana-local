#!/usr/bin/env python3

# Grafana 11 compatible dynamic dashboard provisioning

# Author: Joel Antony (SimplyAI) - Final Stable Version (working identical to manual code)

import os, sys, json, requests, re

# -------------------------------
# Environment Variables
# -------------------------------

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000").rstrip("/")
GRAFANA_USER = os.environ.get("GRAFANA_USER", "admin")
GRAFANA_PASS = os.environ.get("GRAFANA_PASS", "Pwd4simplyai!")
DS_NAME = os.environ.get("DS_NAME", "Postgres Insurance (internal)")
PG_HOST = os.environ.get("PG_HOST", "postgres:5432")
PG_DB = os.environ.get("PG_DB", "insurance")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASS = os.environ.get("PG_PASS", "postgres")
FOLDER_TITLE = "MLVizz"
GRAFANA_TIMEOUT = int(os.environ.get("GRAFANA_TIMEOUT", "15"))

# -------------------------------
# Grafana HTTP Session
# -------------------------------

s = requests.Session()
s.auth = (GRAFANA_USER, GRAFANA_PASS)
s.headers.update({"Content-Type": "application/json"})

def gget(p):
    r = s.get(f"{GRAFANA_URL}{p}", timeout=GRAFANA_TIMEOUT); r.raise_for_status(); return r
def gpost(p,d):
    r = s.post(f"{GRAFANA_URL}{p}", data=json.dumps(d), timeout=GRAFANA_TIMEOUT); r.raise_for_status(); return r

# -------------------------------
# Ensure Data Source
# -------------------------------

def ensure_ds():
    try:
        r = gget(f"/api/datasources/name/{DS_NAME}")
        ds = r.json()
        print("[i] datasource exists:", ds.get("uid"))
        return ds["uid"]
    except Exception:
        pass
    payload = {
        "name": DS_NAME, "type": "postgres", "access": "proxy",
        "url": PG_HOST, "user": PG_USER, "database": PG_DB,
        "basicAuth": False, "isDefault": False,
        "jsonData": {"sslmode": "disable"},
        "secureJsonData": {"password": PG_PASS},
    }
    ds = gpost("/api/datasources", payload).json()["datasource"]
    print("[+] datasource created:", ds.get("uid"))
    return ds["uid"]

# -------------------------------
# Ensure Folder
# -------------------------------

def ensure_folder():
    for f in gget("/api/folders").json():
        if f["title"] == FOLDER_TITLE: return f["id"]
    return gpost("/api/folders", {"title": FOLDER_TITLE}).json()["id"]

# -------------------------------
# WHERE clause helpers (fixed to match manual logic)
# -------------------------------

def _where_snippet_for_dropdown(var_name, target_column):
    """
    Safe for numeric columns too: cast to text.
    Works for ALL and multi-selects.
    """
    return (
        f"((POSITION('ALL' IN '${{{var_name}}}') > 0) "
        f"OR {target_column}::text = ANY(string_to_array('${{{var_name}}}', ',')))"
    )

def _apply_where_links(base_sql, comp_id, cfg, date_macro_col="claims.claimdate"):
    print("[DEBUG] Applying WHERE for:", base_sql[:90])
    where_parts = []
    for link in cfg.get("filter_links", []) or []:
        for tgt in link.get("targets", []):
            if tgt.get("target_component_id") != comp_id:
                continue
            src_id = link.get("source_component_id")
            src = next((c for c in cfg.get("components", []) if c.get("id") == src_id), None)
            if not src: continue
            if src["type"] == "dropdown_filter":
                var_name = src["filter_key"]
                target_col = tgt["target_query_column_to_filter"]
                where_parts.append(_where_snippet_for_dropdown(var_name, target_col))
            elif src["type"] == "date_range_filter":
                target_col = tgt.get("target_query_column_to_filter", date_macro_col)
                where_parts.append(f"$__timeFilter({target_col})")
    result_sql = base_sql.strip().rstrip(";")
    if "##WHERE_CLAUSE##" not in result_sql:
        result_sql += " ##WHERE_CLAUSE##"
    if not where_parts:
        result = result_sql.replace("##WHERE_CLAUSE##", "")
        print("[DEBUG FINAL SQL]", result)
        return result
    conj = " AND ".join(where_parts)
    tmp_for_detection = result_sql.replace("##WHERE_CLAUSE##", "")
    has_real_where = re.search(r"\bWHERE\b", tmp_for_detection, flags=re.IGNORECASE) is not None
    if has_real_where:
        result = result_sql.replace("##WHERE_CLAUSE##", f" AND {conj}")
    else:
        result = result_sql.replace("##WHERE_CLAUSE##", f" WHERE {conj}")
    print("[DEBUG FINAL SQL]", result)
    return result

# -------------------------------
# Variable builder
# -------------------------------

def build_variables(components, sqls, ds_uid):
    vars_out = []
    for c in components or []:
        if c["type"] != "dropdown_filter": continue
        name = c["filter_key"]; label = c.get("title", name)
        table_name = c.get("table_name")
        qkey = c.get("query_key_options")
        sql = sqls.get(qkey) if qkey in (sqls or {}) else None
        if not sql and table_name:
            sql = f"SELECT DISTINCT {name} AS __text, {name} AS __value FROM {table_name}"
        if not sql:
            # Skip variable if we cannot determine a query safely
            continue
        vars_out.append({
            "type": "query", "name": name, "label": label, "hide": 0,
            "datasource": {"type": "postgres", "uid": ds_uid},
            "definition": sql, "query": sql, "refresh": 2,
            "includeAll": True, "allValue": "ALL", "multi": True,
            "current": {"text": ["ALL"], "value": ["ALL"]}, "options": []
        })
    return vars_out

# -------------------------------
# Panel builder
# -------------------------------

def _panel_from_component(comp, ds_uid, sqls, cfg):
    comp_id = comp["id"]; title = comp.get("title", comp_id)
    ctype = comp["type"]; qkey = comp.get("query_key_data")
    base_sql = None
    if qkey and isinstance(sqls, dict):
        base_sql = sqls.get(qkey)
    if not base_sql:
        tn = comp.get('table_name','')
        base_sql = f"SELECT COUNT(*) FROM {tn}" if tn else "SELECT 1"
    sql = _apply_where_links(base_sql, comp_id, cfg).strip()
    ds = {"type": "postgres", "uid": ds_uid}
    target = {"refId": "A", "rawSql": sql, "format": "table"}
    if ctype == "kpi":
        return {"type": "stat", "title": title, "datasource": ds,
                "targets": [target], "options": {"reduceOptions": {"calcs": ["lastNotNull"]}}}
    elif ctype == "bar_chart":
        return {"type": "barchart", "title": title, "datasource": ds,
                "targets": [target], "options": {"legend": {"displayMode": "list", "placement": "bottom"}}}
    elif ctype == "line_chart":
        target_ts = {"refId": "A", "rawSql": sql, "format": "time_series"}
        return {"type": "timeseries", "title": title, "datasource": ds,
                "targets": [target_ts], "options": {"legend": {"displayMode": "list", "placement": "bottom"}}}
    else:
        return {"type": "table", "title": title, "datasource": ds, "targets": [target]}

# -------------------------------
# Layout helper
# -------------------------------

def _grid_for_layout(layout):
    mapping = {}; y = 0
    for section in (layout or []):
        x = 0; row_height = 0
        for col in section.get("columns", []) or []:
            if "component_id" not in col: continue
            width_units = col.get("width", 6)
            try:
                width_units = int(width_units)
            except Exception:
                width_units = 6
            w = min(24, max(1, width_units) * 2)
            comp_id = col["component_id"]
            h = 6 if w <= 8 else 10; mapping[comp_id] = {"x": x, "y": y, "w": w, "h": h}
            x = min(24, x + w); row_height = max(row_height, h)
        y += row_height or 6
    return mapping

# -------------------------------
# Dashboard builder
# -------------------------------

def build_dashboard(cfg, ds_uid):
    components = cfg.get("components", [])
    sqls = cfg.get("sql_queries", {})
    layout = cfg.get("layout_structure", [])
    settings = cfg.get("dashboard_settings", {})
    variables = build_variables(components, sqls, ds_uid)
    grid_map = _grid_for_layout(layout)
    panels = []
    for c in components or []:
     if c["type"] in ("kpi", "bar_chart", "line_chart", "table_chart"):
        p = _panel_from_component(c, ds_uid, sqls, cfg)
        p["gridPos"] = grid_map.get(c["id"], {"x":0,"y":0,"w":24,"h":8})
        panels.append(p)
    time_cfg = settings.get("time") if isinstance(settings.get("time"), dict) else None
    refresh = settings.get("refresh") or "1m"
    title = settings.get("title", "Generated Dashboard")
    return {"id": None, "uid": None,
            "title": title,
            "timezone": "browser", "editable": True,
            "refresh": refresh, "time": (time_cfg or {"from":"now-3y","to":"now"}),
            "templating": {"list": variables}, "panels": panels}

# -------------------------------
# Upsert
# -------------------------------

def upsert(fid, dash):
    payload = {"dashboard": dash, "folderId": fid, "overwrite": True, "message": "auto-provision"}
    j = gpost("/api/dashboards/db", payload).json()
    print("[+] dashboard upserted:", j.get("url","(no URL)"))

# -------------------------------
# Main
# -------------------------------

def main():
    cfg_path = None
    if len(sys.argv) >= 2:
        cfg_path = sys.argv[1]
    else:
        cfg_path = os.environ.get("CONFIG_PATH")
    if not cfg_path:
        print("Usage: python3 provision_grafana_mlvizz.py example.json"); sys.exit(1)
    if not os.path.exists(cfg_path):
        print(f"[!] Config file not found: {cfg_path}"); sys.exit(1)
    with open(cfg_path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    print("[i] checking Grafana health…"); print(gget("/api/health").json())
    ds_uid = ensure_ds(); fid = ensure_folder()
    dash = build_dashboard(cfg, ds_uid); upsert(fid, dash)
    print("[\u2713] Done. Open Grafana → Dashboards → MLVizz →", dash["title"])

if __name__ == "__main__": main()