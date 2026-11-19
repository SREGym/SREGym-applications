#!/usr/bin/env python3
"""Regional customer usage, settlement, partner projection and operations index."""

import csv
import io
import json
import logging
import os
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pg8000.dbapi
from kafka import KafkaConsumer

REGION = os.environ["REGION"]
PG = os.environ["PG_HOST"]
PORT = int(os.environ.get("PORT", "8080"))
BOOT = os.environ["KAFKA_BOOTSTRAP"]
CACHE = {}
LOCK = threading.Lock()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def db():
    return pg8000.dbapi.connect(host=PG, port=31432, user="postgres", database="ledger", timeout=10)


def sql(query, args=()):
    c = db()
    try:
        cur = c.cursor()
        cur.execute(query, args)
        rows = cur.fetchall() if cur.description else []
        c.commit()
        return rows
    finally:
        c.close()


SCHEMA = """CREATE TABLE IF NOT EXISTS customer_accounts (tenant text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS device_usage (source text, day bigint, readings bigint, quantity bigint, revision bigint, received_at timestamptz DEFAULT now(), PRIMARY KEY(source,day));
CREATE TABLE IF NOT EXISTS partner_revisions (topic text, partition integer, offset_id bigint, origin text, tenant text, hour bigint, revision bigint, readings bigint, quantity bigint, received_at timestamptz DEFAULT now(), PRIMARY KEY(topic,partition,offset_id));
CREATE TABLE IF NOT EXISTS partner_totals (origin text,tenant text,hour bigint,readings bigint,quantity bigint,revision bigint,PRIMARY KEY(origin,tenant,hour));
CREATE TABLE IF NOT EXISTS closed_reports (tenant text,hour bigint,readings bigint,quantity bigint,closed_at timestamptz DEFAULT now(),PRIMARY KEY(tenant,hour));
CREATE TABLE IF NOT EXISTS product_checks (at timestamptz DEFAULT now(),check_name text,tenant text,hour bigint,left_value bigint,right_value bigint);
CREATE TABLE IF NOT EXISTS reporting_settings (name text PRIMARY KEY,value text);
INSERT INTO reporting_settings VALUES ('partner_mode','latest') ON CONFLICT DO NOTHING;
"""


def initialize():
    c = db()
    cur = c.cursor()
    for q in SCHEMA.split(";"):
        if q.strip():
            cur.execute(q)
    for i in range(1, 21):
        cur.execute("INSERT INTO customer_accounts VALUES (%s) ON CONFLICT DO NOTHING", (f"tenant-{REGION}-{i:02d}",))
    c.commit()
    c.close()


def partner():
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                "rollup-" + REGION,
                ("us" if REGION == "eu" else "eu") + ".rollup-" + ("us" if REGION == "eu" else "eu"),
                bootstrap_servers=BOOT.split(","),
                group_id="partner-reporting-" + REGION,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                max_poll_records=100,
            )
            for m in consumer:
                r = json.loads(m.value)
                c = db()
                cur = c.cursor()
                cur.execute(
                    "INSERT INTO partner_revisions VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now()) ON CONFLICT DO NOTHING RETURNING offset_id",
                    (
                        m.topic,
                        m.partition,
                        m.offset,
                        r["origin_region"],
                        r["tenant"],
                        r["hour"],
                        r["revision"],
                        r["readings"],
                        r["quantity"],
                    ),
                )
                inserted = cur.fetchone()
                cur.execute("SELECT value FROM reporting_settings WHERE name='partner_mode'")
                mode = cur.fetchone()[0]
                if inserted:
                    if mode == "latest":
                        cur.execute(
                            "INSERT INTO partner_totals VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(origin,tenant,hour) DO UPDATE SET readings=excluded.readings,quantity=excluded.quantity,revision=excluded.revision WHERE excluded.revision>=partner_totals.revision",
                            (r["origin_region"], r["tenant"], r["hour"], r["readings"], r["quantity"], r["revision"]),
                        )
                    else:
                        cur.execute(
                            "INSERT INTO partner_totals VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(origin,tenant,hour) DO UPDATE SET readings=partner_totals.readings+excluded.readings,quantity=partner_totals.quantity+excluded.quantity,revision=greatest(partner_totals.revision,excluded.revision)",
                            (r["origin_region"], r["tenant"], r["hour"], r["readings"], r["quantity"], r["revision"]),
                        )
                c.commit()
                c.close()
        except Exception:
            logging.exception("partner reader unavailable")
            time.sleep(5)
        finally:
            if consumer:
                consumer.close()


def maintenance():
    while True:
        try:
            c = db()
            cur = c.cursor()
            # Settlement is immutable after an hour closes; ongoing reads remain live.
            cur.execute(
                "INSERT INTO closed_reports(tenant,hour,readings,quantity) SELECT a.tenant,extract(epoch FROM date_trunc('hour',now())-interval '1 hour')::bigint,count(r.id),coalesce(sum(r.quantity),0) FROM customer_accounts a LEFT JOIN readings r ON r.tenant=a.tenant AND r.event_time>=date_trunc('hour',now())-interval '1 hour' AND r.event_time<date_trunc('hour',now()) GROUP BY a.tenant ON CONFLICT DO NOTHING"
            )
            cur.execute(
                "INSERT INTO product_checks(check_name,tenant,hour,left_value,right_value) SELECT 'settlement_and_ledger',s.tenant,s.hour,s.readings,l.n FROM closed_reports s JOIN (SELECT tenant,extract(epoch FROM date_trunc('hour',event_time))::bigint AS hour,count(*) n FROM readings GROUP BY 1,2) l USING(tenant,hour) WHERE s.readings<>l.n"
            )
            cur.execute("DELETE FROM product_checks WHERE at<now()-interval '7 days'")
            c.commit()
            c.close()
        except Exception:
            logging.exception("report maintenance failed")
        time.sleep(30)


def args(query):
    q = urllib.parse.parse_qs(query)

    def one(k, d):
        return q.get(k, [d])[0]

    day = int(time.time() // 86400) * 86400
    return dict(
        tenant=one("tenant", ""),
        start=float(one("start", str(day))),
        end=float(one("end", str(day + 86400))),
        timezone=one("timezone", "UTC"),
        fresh=one("fresh", "false") == "true",
    )


def bounds(a):
    if not 0 < a["end"] - a["start"] <= 8 * 86400:
        raise ValueError("Query interval must be positive and at most eight days")
    return (a["start"], a["end"], a["tenant"], a["tenant"])


def reports(a, utc=False):
    params = bounds(a)
    tz = "UTC" if utc else a["timezone"]
    rows = sql(
        "SELECT tenant,extract(epoch FROM date_trunc('hour',event_time AT TIME ZONE %s))::bigint,count(*),sum(quantity),count(*)-count(DISTINCT (device_id,boot_id,seq)) FROM readings WHERE event_time>=to_timestamp(%s) AND event_time<to_timestamp(%s) AND (%s='' OR tenant=%s) GROUP BY 1,2 ORDER BY 1,2",
        (tz, *params),
    )
    return dict(
        region=REGION,
        timezone=tz,
        bucket_unit="UTC epoch seconds",
        rows=[dict(tenant=r[0], hour=r[1], readings=r[2], quantity=int(r[3]), duplicate_readings=r[4]) for r in rows],
    )


HOME = """<!doctype html><html><head><title>Smart Meter Portal</title><meta charset="utf-8"><style>body{font:16px system-ui;max-width:1000px;margin:45px auto;color:#182235}nav a{margin-right:20px}input,select,button{padding:9px;margin:5px}pre{background:#f3f5f8;padding:20px;white-space:pre-wrap}</style></head><body><h1>Smart Meter Portal</h1><p>Customer usage and partner reporting</p><nav><a href="/api/accounts">Accounts</a><a href="/api/live">Live devices</a><a href="/api/contract">Product contract</a><a href="/api/tools">Company tools</a><a href="/api/checks">Product checks</a></nav><form id="q">Tenant <input name="tenant" placeholder="All accounts">Timezone <select name="timezone"><option>UTC</option><option>America/Los_Angeles</option><option>Europe/Berlin</option><option>Asia/Tokyo</option></select><button>Load reports</button></form><pre id="result">Select an account or load the current reporting interval.</pre><script>q.onsubmit=async e=>{e.preventDefault();result.textContent=JSON.stringify(await(await fetch('/api/reports?'+new URLSearchParams(new FormData(q)))).json(),null,2)}</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, data, html=False):
        payload = data.encode() if html else json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8" if html else "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        try:
            u = urllib.parse.urlparse(self.path)
            a = args(u.query)
            if u.path == "/":
                return self.reply(200, HOME, True)
            if u.path == "/health":
                return self.reply(200, dict(region=REGION, database=sql("SELECT 1")[0][0], time=time.time()))
            if u.path == "/metrics":
                readings = sql("SELECT count(*) FROM readings")[0][0]
                revisions = sql("SELECT count(*) FROM partner_revisions")[0][0]
                payload = f'company_ledger_readings{{region="{REGION}"}} {readings}\ncompany_partner_revisions{{region="{REGION}"}} {revisions}\ncompany_report_cache_entries{{region="{REGION}"}} {len(CACHE)}\ncompany_clock_seconds {time.time()}\n'.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if u.path == "/api/live":
                from company_mqtt_latest import snapshot

                host = "10.72.0.13" if REGION == "us" else "10.72.0.21"
                return self.reply(
                    200, dict(region=REGION, view="Latest retained browser-device readings", rows=snapshot(host))
                )
            if u.path == "/api/accounts":
                return self.reply(200, [r[0] for r in sql("SELECT tenant FROM customer_accounts ORDER BY 1")])
            if u.path == "/api/contract":
                return self.reply(
                    200,
                    dict(
                        product="Hourly customer and partner usage",
                        horizon_days=7,
                        event_time="UTC epoch seconds",
                        identity=["device_id", "boot_id", "seq"],
                        reporting="Event-hour counts and quantities; pending data may revise live reports. Settled reports are used by the billing close.",
                        billing="Every reading a device was told was accepted is counted exactly once, in its own region's ledger, with the values the device sent.",
                        freshness="Every reading a device sends is in its region's ledger within 120 seconds, and customer reporting is available in both regions.",
                        statements="For every closed hour, settled reports and both regions' partner statements equal the ledger.",
                        continuity="These commitments hold when the response ends and keep holding through the company's routine automation.",
                    ),
                )
            if u.path == "/api/tools":
                return self.reply(
                    200,
                    dict(
                        source="http://10.72.0.23:3000/",
                        artifacts="http://10.72.0.23:8091/catalog.json",
                        metrics="http://10.72.0.20:9090/",
                        logs="http://10.72.0.22:3100/",
                        regions=dict(us="https://10.72.0.10:6443", eu="https://10.72.0.20:6443"),
                        access="companyctl provides the authorized provider and guest shell/console operations",
                    ),
                )
            if u.path == "/api/reports":
                key = (a["tenant"], a["start"], a["end"])
                with LOCK:
                    cached = CACHE.get(key)
                    if not a["fresh"] and cached and time.monotonic() - cached[0] < 180:
                        return self.reply(200, {**cached[1], "cache": "hit"})
                result = reports(a)
                with LOCK:
                    CACHE[key] = (time.monotonic(), result)
                return self.reply(200, {**result, "cache": "fresh"})
            if u.path == "/api/ledger":
                return self.reply(200, reports(a, True))
            if u.path == "/api/export.csv":
                output = io.StringIO()
                writer = csv.DictWriter(
                    output, fieldnames=["tenant", "hour", "readings", "quantity", "duplicate_readings"]
                )
                writer.writeheader()
                writer.writerows(reports(a, True)["rows"])
                payload = output.getvalue().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.send_header("Content-Disposition", "attachment; filename=usage.csv")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if u.path == "/api/usage":
                bounds(a)
                rows = sql(
                    "SELECT row_to_json(t) FROM (SELECT day,sum(readings) AS readings,sum(quantity) AS quantity FROM device_usage WHERE day>=%s AND day<%s GROUP BY day ORDER BY day) t",
                    (int(a["start"] // 86400) * 86400, int((a["end"] + 86399) // 86400) * 86400),
                )
                return self.reply(200, dict(region=REGION, unit="UTC day", rows=[r[0] for r in rows]))
            if u.path in ["/api/partner", "/api/revisions", "/api/closed"]:
                bounds(a)
                table = {
                    "/api/partner": "partner_totals",
                    "/api/revisions": "partner_revisions",
                    "/api/closed": "closed_reports",
                }[u.path]
                rows = sql(
                    "SELECT row_to_json(t) FROM (SELECT * FROM "
                    + table
                    + " WHERE hour>=%s AND hour<%s AND (%s='' OR tenant=%s) ORDER BY hour,tenant LIMIT 10000) t",
                    (int(a["start"] // 3600) * 3600, int((a["end"] + 3599) // 3600) * 3600, a["tenant"], a["tenant"]),
                )
                return self.reply(200, [r[0] for r in rows])
            if u.path == "/api/checks":
                return self.reply(
                    200,
                    [
                        r[0]
                        for r in sql(
                            "SELECT row_to_json(t) FROM (SELECT * FROM product_checks ORDER BY at DESC LIMIT 500) t"
                        )
                    ],
                )
            return self.reply(404, {"error": "Unknown resource"})
        except ValueError as e:
            self.reply(400, {"error": str(e)})
        except Exception as e:
            logging.exception("request failed")
            self.reply(503, {"error": str(e)})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > 1048576:
                return self.reply(413, {"error": "request too large"})
            data = json.loads(self.rfile.read(length) or b"{}")
            p = urllib.parse.urlparse(self.path).path
            if p == "/api/cache/clear":
                with LOCK:
                    CACHE.clear()
                return self.reply(200, {"cleared": True})
            if p == "/api/usage":
                c = db()
                cur = c.cursor()
                for r in data["rows"]:
                    cur.execute(
                        "INSERT INTO device_usage(source,day,readings,quantity,revision) VALUES (%s,%s,%s,%s,%s) ON CONFLICT(source,day) DO UPDATE SET readings=excluded.readings,quantity=excluded.quantity,revision=excluded.revision,received_at=now() WHERE excluded.revision>=device_usage.revision",
                        (data["source"], r["day"], r["readings"], r["quantity"], data["revision"]),
                    )
                c.commit()
                c.close()
                return self.reply(200, {"stored": len(data["rows"])})
            if p == "/api/partner/rebuild":
                mode = data.get("mode", "latest")
                if mode not in ["latest", "sum"]:
                    raise ValueError("mode must be latest or sum")
                c = db()
                cur = c.cursor()
                cur.execute("LOCK TABLE partner_totals,partner_revisions IN EXCLUSIVE MODE")
                cur.execute("DELETE FROM partner_totals")
                if mode == "latest":
                    cur.execute(
                        "INSERT INTO partner_totals SELECT DISTINCT ON(origin,tenant,hour) origin,tenant,hour,readings,quantity,revision FROM partner_revisions ORDER BY origin,tenant,hour,revision DESC,offset_id DESC"
                    )
                else:
                    cur.execute(
                        "INSERT INTO partner_totals SELECT origin,tenant,hour,sum(readings),sum(quantity),max(revision) FROM partner_revisions GROUP BY 1,2,3"
                    )
                cur.execute("UPDATE reporting_settings SET value=%s WHERE name='partner_mode'", (mode,))
                c.commit()
                c.close()
                return self.reply(200, {"rebuilt": True, "mode": mode})
            if p == "/api/closed/recompute":
                a = {**args(""), **data}
                b = bounds(a)
                sql(
                    "INSERT INTO closed_reports(tenant,hour,readings,quantity) SELECT tenant,extract(epoch FROM date_trunc('hour',event_time))::bigint,count(*),sum(quantity) FROM readings WHERE event_time>=to_timestamp(%s) AND event_time<to_timestamp(%s) AND (%s='' OR tenant=%s) GROUP BY 1,2 ON CONFLICT(tenant,hour) DO UPDATE SET readings=excluded.readings,quantity=excluded.quantity,closed_at=now()",
                    b,
                )
                return self.reply(200, {"recomputed": True})
            return self.reply(404, {"error": "Unknown operation"})
        except (ValueError, KeyError) as e:
            self.reply(400, {"error": str(e)})
        except Exception as e:
            logging.exception("operation failed")
            self.reply(503, {"error": str(e)})


if __name__ == "__main__":
    initialize()
    threading.Thread(target=partner, daemon=True).start()
    threading.Thread(target=maintenance, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
