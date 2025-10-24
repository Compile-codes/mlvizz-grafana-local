#!/usr/bin/env python3
# Grafana 11 compatible MLVizz dashboard provisioning
#(One-time) install the Python dependency: python3 -m pip install --upgrade requests
#Author: Joel Antony - Data Engineer Intern - Simplyai

import os, json, requests

GRAFANA_URL=os.environ.get("GRAFANA_URL","http://localhost:3000").rstrip("/")
GRAFANA_USER=os.environ.get("GRAFANA_USER","admin")
GRAFANA_PASS=os.environ.get("GRAFANA_PASS","admin")
DS_NAME=os.environ.get("DS_NAME","Postgres Insurance (internal)")
PG_HOST=os.environ.get("PG_HOST","postgres:5432")
PG_DB=os.environ.get("PG_DB","insurance")
PG_USER=os.environ.get("PG_USER","postgres")
PG_PASS=os.environ.get("PG_PASS","postgres")
FOLDER_TITLE="MLVizz"
DASHBOARD_TITLE="MLVizz Insurance Overview"

s=requests.Session();s.auth=(GRAFANA_USER,GRAFANA_PASS);s.headers.update({"Content-Type":"application/json"})
def gget(p):r=s.get(f"{GRAFANA_URL}{p}");r.raise_for_status();return r
def gpost(p,d):r=s.post(f"{GRAFANA_URL}{p}",data=json.dumps(d));r.raise_for_status();return r

def ensure_ds():
    try:r=gget(f"/api/datasources/name/{DS_NAME}");ds=r.json();print("[i] DS exists",ds.get("uid"));return ds["uid"]
    except:pass
    d={"name":DS_NAME,"type":"postgres","access":"proxy","url":PG_HOST,"user":PG_USER,"database":PG_DB,"basicAuth":False,"isDefault":False,"jsonData":{"sslmode":"disable"},"secureJsonData":{"password":PG_PASS}}
    ds=gpost("/api/datasources",d).json()["datasource"];print("[+] DS created",ds.get("uid"));return ds["uid"]
def ensure_folder():
    for f in gget("/api/folders").json():
        if f["title"]==FOLDER_TITLE:return f["id"]
    return gpost("/api/folders",{"title":FOLDER_TITLE}).json()["id"]

def build(ds_uid):
    vars=[
        {"name":"insurancetype","label":"Insurance Type","type":"query","datasource":{"type":"postgres","uid":ds_uid},"definition":"SELECT DISTINCT insurancetype AS __text, insurancetype AS __value FROM policies ORDER BY insurancetype;","query":"SELECT DISTINCT insurancetype AS __text, insurancetype AS __value FROM policies ORDER BY insurancetype;","includeAll":True,"multi":True},
        {"name":"status","label":"Status","type":"query","datasource":{"type":"postgres","uid":ds_uid},"definition":"SELECT DISTINCT status AS __text, status AS __value FROM claims ORDER BY status;","query":"SELECT DISTINCT status AS __text, status AS __value FROM claims ORDER BY status;","includeAll":True,"multi":True},
        {"name":"agentid","label":"Agent","type":"query","datasource":{"type":"postgres","uid":ds_uid},"definition":"SELECT agentname AS __text, agentid AS __value FROM agents ORDER BY agentname;","query":"SELECT agentname AS __text, agentid AS __value FROM agents ORDER BY agentname;","includeAll":True,"multi":True}
    ]
    def stat(title,q,x,y):return{"type":"stat","title":title,"datasource":{"type":"postgres","uid":ds_uid},"gridPos":{"h":6,"w":8,"x":x,"y":y},"targets":[{"refId":"A","rawSql":q,"format":"table"}]}
    def bar(title,q,x,y):return{"type":"barchart","title":title,"datasource":{"type":"postgres","uid":ds_uid},"gridPos":{"h":10,"w":12,"x":x,"y":y},"targets":[{"refId":"A","rawSql":q,"format":"table"}]}
    def ts(title,q,x,y):return{"type":"timeseries","title":title,"datasource":{"type":"postgres","uid":ds_uid},"gridPos":{"h":10,"w":24,"x":x,"y":y},"targets":[{"refId":"A","rawSql":q,"format":"time_series"}]}
    tp="""SELECT COUNT(*) AS total_count FROM policies WHERE (ARRAY['ALL']&&ARRAY[${insurancetype}] OR insurancetype=ANY(ARRAY[${insurancetype}])) AND (ARRAY['ALL']&&ARRAY[${agentid}] OR policies.agentid::text=ANY(ARRAY[${agentid}]));"""
    tc="""SELECT COUNT(*) AS total_count FROM claims WHERE (ARRAY['ALL']&&ARRAY[${status}] OR status=ANY(ARRAY[${status}])) AND $__timeFilter(claimdate);"""
    ti="SELECT COUNT(*) AS total_count FROM insured_items;"
    pbt="""SELECT insurancetype AS label,COUNT(*) AS count FROM policies WHERE (ARRAY['ALL']&&ARRAY[${insurancetype}] OR insurancetype=ANY(ARRAY[${insurancetype}])) AND (ARRAY['ALL']&&ARRAY[${agentid}] OR policies.agentid::text=ANY(ARRAY[${agentid}])) GROUP BY insurancetype ORDER BY count DESC;"""
    cbs="""SELECT status AS label,COUNT(*) AS count FROM claims WHERE (ARRAY['ALL']&&ARRAY[${status}] OR status=ANY(ARRAY[${status}])) AND $__timeFilter(claimdate) GROUP BY status ORDER BY count DESC;"""
    cot="""SELECT DATE_TRUNC('month',claimdate) AS time,COUNT(*) AS count FROM claims WHERE (ARRAY['ALL']&&ARRAY[${status}] OR status=ANY(ARRAY[${status}])) AND $__timeFilter(claimdate) GROUP BY 1 ORDER BY 1;"""
    panels=[stat("Total Policies",tp,0,0),stat("Total Claims",tc,8,0),stat("Total Insured Items",ti,16,0),bar("Policies by Insurance Type",pbt,0,6),bar("Claims by Status",cbs,12,6),ts("Claims Over Time",cot,0,16)]
    return{"id":None,"uid":None,"title":DASHBOARD_TITLE,"editable":True,"timezone":"browser","templating":{"list":vars},"time":{"from":"now-3y","to":"now"},"refresh":"1m","panels":panels}

def upsert(fid,d):
    r=gpost("/api/dashboards/db",{"dashboard":d,"folderId":fid,"overwrite":True,"message":"v2 update"}).json()
    print("[+] Dashboard created",r.get("url"))

if __name__=="__main__":
    print("[i] Checking Grafana health");print(gget("/api/health").json())
    uid=ensure_ds();fid=ensure_folder();dash=build(uid);upsert(fid,dash);print("[✓] Open Grafana → Dashboards → MLVizz → MLVizz Insurance Overview")
