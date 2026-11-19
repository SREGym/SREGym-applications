"""Publish and deploy the regional reporting profiles on fresh stores."""

import ast
import json
import pathlib
import subprocess

p = pathlib.Path("/var/lib/company/platform-source")
r = p / "services/reporting.py"
r.write_text(ast.unparse(ast.parse(r.read_text())) + "\n")
# Reporting and partner clients read these profiles.
profiles = {
    "us": dict(
        cache_ttl=240,
        maintenance_interval=45,
        partner_max_poll_records=80,
        partner_fetch_min_bytes=1,
        partner_fetch_max_wait_ms=250,
        partner_auto_commit_interval_ms=4000,
        partner_session_timeout_ms=12000,
        partner_heartbeat_interval_ms=3000,
        partner_receive_buffer_bytes=65536,
        partner_send_buffer_bytes=65536,
        pg_timeout=12,
        pg_statement_timeout_ms=20000,
        http_poll_interval_ms=5000,
    ),
    "eu": dict(
        cache_ttl=90,
        maintenance_interval=20,
        partner_max_poll_records=150,
        partner_fetch_min_bytes=4096,
        partner_fetch_max_wait_ms=500,
        partner_auto_commit_interval_ms=7000,
        partner_session_timeout_ms=18000,
        partner_heartbeat_interval_ms=4000,
        partner_receive_buffer_bytes=131072,
        partner_send_buffer_bytes=131072,
        pg_timeout=18,
        pg_statement_timeout_ms=30000,
        http_poll_interval_ms=8000,
    ),
}
extra_profiles = {
    "us": dict(
        metrics_num_samples=2,
        metrics_sample_window_ms=15000,
        retry_backoff_ms=100,
        reconnect_backoff_ms=50,
        reconnect_backoff_max_ms=1000,
        request_timeout_ms=30000,
        metadata_max_age_ms=300000,
        connections_max_idle_ms=540000,
        fetch_max_bytes=16777216,
        max_partition_fetch_bytes=1048576,
        api_version_auto_timeout_ms=10000,
        max_poll_interval_ms=120000,
    ),
    "eu": dict(
        metrics_num_samples=3,
        metrics_sample_window_ms=30000,
        retry_backoff_ms=250,
        reconnect_backoff_ms=100,
        reconnect_backoff_max_ms=2000,
        request_timeout_ms=40000,
        metadata_max_age_ms=180000,
        connections_max_idle_ms=300000,
        fetch_max_bytes=25165824,
        max_partition_fetch_bytes=2097152,
        api_version_auto_timeout_ms=15000,
        max_poll_interval_ms=180000,
    ),
}
for region in profiles:
    profiles[region]["partner_options"] = extra_profiles[region]
(p / "fleet/application-profiles.json").write_text(json.dumps(profiles, indent=2) + "\n")
r = p / "services/reporting.py"
s = r.read_text()
# The source was normalized with ast.unparse above.
anchor = "REGION = os.environ['REGION']"
if anchor not in s:
    raise RuntimeError("Unexpected reporting source")
s = s.replace(anchor, anchor + "\nPROFILE = json.load(open('/opt/company/application-profile.json'))")
s = s.replace("timeout=10)", "timeout=PROFILE['pg_timeout'])")
s = s.replace(
    "return pg8000.dbapi.connect(host=PG, port=31432, user='postgres', database='ledger', timeout=PROFILE['pg_timeout'])",
    "c = pg8000.dbapi.connect(host=PG, port=31432, user='postgres', database='ledger', timeout=PROFILE['pg_timeout'])\n    cur = c.cursor()\n    cur.execute('SET statement_timeout TO ' + str(PROFILE['pg_statement_timeout_ms']))\n    c.commit()\n    return c",
)
s = s.replace(
    "max_poll_records=100)",
    "max_poll_records=PROFILE['partner_max_poll_records'], fetch_min_bytes=PROFILE['partner_fetch_min_bytes'], fetch_max_wait_ms=PROFILE['partner_fetch_max_wait_ms'], auto_commit_interval_ms=PROFILE['partner_auto_commit_interval_ms'], session_timeout_ms=PROFILE['partner_session_timeout_ms'], heartbeat_interval_ms=PROFILE['partner_heartbeat_interval_ms'], receive_buffer_bytes=PROFILE['partner_receive_buffer_bytes'], send_buffer_bytes=PROFILE['partner_send_buffer_bytes'], **PROFILE['partner_options'])",
)
s = s.replace("time.sleep(30)", "time.sleep(PROFILE['maintenance_interval'])").replace(
    "< 180)", "< PROFILE['cache_ttl'])"
)
# Status reports the health of devices observed in the ledger.
status = """            if u.path == '/api/status':
                rows=sql("SELECT count(*),count(*) FILTER (WHERE seen>=now()-interval '120 seconds') FROM (SELECT device_id,max(ingested_at) seen FROM readings WHERE ingested_at>=now()-interval '10 minutes' GROUP BY device_id) d")[0]
                return self.reply(200,dict(region=REGION,observed_devices=rows[0],recent_observed_devices=rows[1],fresh=(rows[0]==rows[1]) if rows[0] else None,basis='Most recent stored reading for devices observed in the last ten minutes'))
"""
marker = "            if u.path == '/health':"
if marker not in s:
    raise RuntimeError("Unexpected reporting indentation")
s = s.replace(marker, status + marker)
start = s.index("            if u.path == '/api/checks':")
stop = s.index("\n            return self.reply(404", start)
s = (
    s[:start]
    + """            if u.path == '/api/checks':
                report=reports(a)
                ledger=reports(a,True)
                return self.reply(200,dict(region=REGION,check='report_and_stored_ledger',consistent=(report['rows']==ledger['rows']),report_rows=len(report['rows']),ledger_rows=len(ledger['rows'])))
"""
    + s[stop:]
)
tree = ast.parse(s)
for node in tree.body:
    if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "HOME" for t in node.targets):
        js = "<p id=\"service-status\"></p><script>setInterval(()=>fetch('/api/status').then(r=>r.json()).then(r=>document.getElementById('service-status').textContent=JSON.stringify(r)),POLL_INTERVAL);</script></body>"
        node.value = ast.Call(
            func=ast.Attribute(value=node.value, attr="replace", ctx=ast.Load()),
            args=[
                ast.Constant("</body>"),
                ast.Call(
                    func=ast.Attribute(value=ast.Constant(js), attr="replace", ctx=ast.Load()),
                    args=[
                        ast.Constant("POLL_INTERVAL"),
                        ast.Call(
                            func=ast.Name(id="str", ctx=ast.Load()),
                            args=[
                                ast.Subscript(
                                    value=ast.Name(id="PROFILE", ctx=ast.Load()),
                                    slice=ast.Constant("http_poll_interval_ms"),
                                    ctx=ast.Load(),
                                )
                            ],
                            keywords=[],
                        ),
                    ],
                    keywords=[],
                ),
            ],
            keywords=[],
        )
s = ast.unparse(ast.fix_missing_locations(tree)) + "\n"
r.write_text(s)

subprocess.run(["git", "-C", str(p), "add", "services/reporting.py", "fleet/application-profiles.json"], check=True)
subprocess.run(["git", "-C", str(p), "commit", "-m", "Deploy regional reporting client and cache profiles"], check=True)
subprocess.run(["git", "-C", str(p), "push", "origin", "HEAD:main"], check=True)
print(
    json.dumps(
        {
            "profile_regions": list(profiles),
            "reporting_syntax_valid": True,
            "source_head": subprocess.check_output(["git", "-C", str(p), "rev-parse", "HEAD"], text=True).strip(),
        }
    )
)
