"""
Kasten Metrics Model - Backend
Queries Kasten's internal Prometheus for export/snapshot storage metrics.
Merges with SQLite long-term store for history beyond Prometheus 30d window.
"""

import os
import sqlite3
import logging
import statistics
from contextlib import contextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from kubernetes import client, config

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("kasten-metrics")

PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL",
    "http://prometheus-server.kasten-io.svc/k10/prometheus"
)
KASTEN_NAMESPACE = os.getenv("KASTEN_NAMESPACE", "kasten-io")
DB_PATH = os.getenv("DB_PATH", "/data/metrics.db")

app = FastAPI(title="Kasten Metrics Model", docs_url="/api/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

@contextmanager
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()


def init_db():
    with db() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS daily_snapshots (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                scraped_at    TEXT NOT NULL,
                profile       TEXT NOT NULL,
                physical_bytes REAL NOT NULL,
                logical_bytes  REAL NOT NULL,
                object_store_type TEXT,
                endpoint      TEXT,
                bucket        TEXT
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_profile_date
            ON daily_snapshots (profile, scraped_at)
        """)
        con.commit()
    log.info(f"DB initialised at {DB_PATH}")


def db_history(profile: str, days: int) -> list[dict]:
    """Return daily rows for a profile from SQLite, oldest first."""
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    cutoff_str = datetime.fromtimestamp(cutoff, tz=timezone.utc).strftime("%Y-%m-%d")
    with db() as con:
        rows = con.execute("""
            SELECT scraped_at, physical_bytes, logical_bytes
            FROM daily_snapshots
            WHERE profile = ? AND scraped_at >= ?
            ORDER BY scraped_at ASC
        """, (profile, cutoff_str)).fetchall()
    return [dict(r) for r in rows]


def db_all_profiles() -> list[str]:
    with db() as con:
        rows = con.execute(
            "SELECT DISTINCT profile FROM daily_snapshots"
        ).fetchall()
    return [r["profile"] for r in rows]


def db_latest_per_profile() -> list[dict]:
    """Most recent scraped row per profile — used to fill gaps when Prometheus is empty."""
    with db() as con:
        rows = con.execute("""
            SELECT profile, physical_bytes, logical_bytes,
                   object_store_type, endpoint, bucket, scraped_at
            FROM daily_snapshots
            WHERE id IN (
                SELECT MAX(id) FROM daily_snapshots GROUP BY profile
            )
            ORDER BY physical_bytes DESC
        """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Kubernetes / Profile helpers
# ---------------------------------------------------------------------------

def _k8s_client():
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client.CustomObjectsApi()


def _load_profiles() -> dict:
    profiles = {}
    try:
        api = _k8s_client()
        items = api.list_namespaced_custom_object(
            group="config.kio.kasten.io",
            version="v1alpha1",
            namespace=KASTEN_NAMESPACE,
            plural="profiles",
        ).get("items", [])
        for p in items:
            name = p["metadata"]["name"]
            loc = p.get("spec", {}).get("locationSpec", {})
            obj = loc.get("objectStore", {})
            if obj:
                key = (
                    obj.get("objectStoreType", ""),
                    obj.get("endpoint", ""),
                    obj.get("name", ""),
                )
                profiles[key] = name
            else:
                profiles[("", "", name)] = name
        log.info(f"Loaded {len(profiles)} profiles from CRDs")
    except Exception as e:
        log.warning(f"Could not load profiles from CRDs: {e}")
    return profiles


_PROFILE_MAP: dict = {}
_POLICY_PROFILE_MAP: dict = {}  # policy_name -> profile_name


def _load_policy_profile_map() -> dict:
    """Builds policy_name -> profile_name for policies with an explicit local profile ref."""
    result = {}
    try:
        api = _k8s_client()
        items = api.list_namespaced_custom_object(
            group="config.kio.kasten.io",
            version="v1alpha1",
            namespace=KASTEN_NAMESPACE,
            plural="policies",
        ).get("items", [])
        for p in items:
            policy_name = p["metadata"]["name"]
            for action in p.get("spec", {}).get("actions", []):
                prof = action.get("exportParameters", {}).get("profile", {})
                if prof.get("name"):
                    result[policy_name] = prof["name"]
                    break
        log.info(f"Loaded {len(result)} policy→profile mappings")
    except Exception as e:
        log.warning(f"Could not load policy→profile map: {e}")
    return result


@app.on_event("startup")
async def startup():
    global _PROFILE_MAP, _POLICY_PROFILE_MAP
    init_db()
    _PROFILE_MAP = _load_profiles()
    _POLICY_PROFILE_MAP = _load_policy_profile_map()


# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------

async def prom_query(query: str) -> list:
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
        r.raise_for_status()
        data = r.json()
        if data["status"] != "success":
            raise HTTPException(500, f"Prometheus error: {data}")
        return data["data"]["result"]


async def prom_range(query: str, start: str, end: str, step: str = "1h") -> list:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params={"query": query, "start": start, "end": end, "step": step},
        )
        r.raise_for_status()
        data = r.json()
        if data["status"] != "success":
            raise HTTPException(500, f"Prometheus range error: {data}")
        return data["data"]["result"]


def _profile_label(m: dict) -> str:
    ost = m.get("object_store_type", "")
    ep = m.get("endpoint", "")
    bucket = m.get("bucket", "")
    repo_path = m.get("repo_path", "")

    if ost or ep or bucket:
        key = (ost, ep, bucket)
        if key in _PROFILE_MAP:
            return _PROFILE_MAP[key]
        host = ep.replace("https://", "").replace("http://", "").rstrip("/")
        if host and bucket:
            return f"{host}/{bucket}"
        return bucket or ep or ost

    rp = repo_path.rstrip("/")
    if rp.endswith("/kopia"):
        rp = rp[: -len("/kopia")]
    # Local snapshot repos leaking into export metric — exclude
    if rp.startswith("repo/"):
        return None
    if rp.startswith("export-"):
        return "manual-exports"
    # Named policy repo — look up policy->profile map first
    if rp in _POLICY_PROFILE_MAP:
        return _POLICY_PROFILE_MAP[rp]
    return rp or "unknown"


def _bytes_to_human(b: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} PB"


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    with db() as con:
        row_count = con.execute("SELECT COUNT(*) FROM daily_snapshots").fetchone()[0]
    return {"status": "ok", "prometheus": PROMETHEUS_URL, "db_rows": row_count}


@app.get("/api/profiles")
async def list_profiles():
    """Current physical size per export profile, with dedup ratio."""
    physical = await prom_query('export_storage_size_bytes{type="physical"}')
    logical  = await prom_query('export_storage_size_bytes{type="logical"}')

    phys_map: dict[str, float] = {}
    phys_meta: dict[str, dict] = {}
    for r in physical:
        m = r["metric"]
        label = _profile_label(m)
        if label is None:
            continue
        val = float(r["value"][1])
        phys_map[label] = phys_map.get(label, 0) + val
        if label not in phys_meta:
            phys_meta[label] = {
                "object_store_type": m.get("object_store_type", ""),
                "endpoint": m.get("endpoint", ""),
                "bucket": m.get("bucket", ""),
                "category": m.get("category", ""),
            }

    log_map: dict[str, float] = {}
    for r in logical:
        m = r["metric"]
        label = _profile_label(m)
        if label is None:
            continue
        log_map[label] = log_map.get(label, 0) + float(r["value"][1])

    # Supplement with latest DB rows for any profile not in Prometheus
    # (covers VBR/SOBR and any other non-Prometheus-tracked profiles)
    for row in db_latest_per_profile():
        p = row["profile"]
        if p not in phys_map:
            phys_map[p] = row["physical_bytes"]
            log_map[p] = row["logical_bytes"]
            phys_meta[p] = {
                "object_store_type": row.get("object_store_type", ""),
                "endpoint": row.get("endpoint", ""),
                "bucket": row.get("bucket", ""),
                "category": "",
            }

    result = []
    for profile, phys in sorted(phys_map.items(), key=lambda x: -x[1]):
        log_val = log_map.get(profile, 0)
        ratio = round(log_val / phys, 3) if phys > 0 else 1.0
        meta = phys_meta.get(profile, {})
        result.append({
            "profile": profile,
            "physical_bytes": phys,
            "logical_bytes": log_val,
            "physical_human": _bytes_to_human(phys),
            "logical_human": _bytes_to_human(log_val),
            "dedup_ratio": ratio,
            "object_store_type": meta.get("object_store_type", ""),
            "endpoint": meta.get("endpoint", ""),
            "bucket": meta.get("bucket", ""),
            "category": meta.get("category", ""),
        })
    return result


@app.get("/api/history")
async def history(
    days: int = Query(default=90, ge=1, le=1825),
    step: str = Query(default="1d"),
):
    """
    Merged time-series: SQLite for older data, Prometheus for recent 30d.
    days param can go up to 1825 (5 years).
    """
    now = int(datetime.now(timezone.utc).timestamp())
    prom_window = 29  # days we trust Prometheus for (leave 1d margin)
    prom_cutoff = now - prom_window * 86400

    merged: dict[str, dict[float, float]] = {}

    # ── SQLite: anything older than prom_window ──────────────────────────────
    if days > prom_window:
        db_days = days  # pull full requested range from DB
        all_profiles = db_all_profiles()
        for profile in all_profiles:
            rows = db_history(profile, db_days)
            if profile not in merged:
                merged[profile] = {}
            for row in rows:
                # Convert date string to unix ts (noon UTC to avoid DST issues)
                dt = datetime.strptime(row["scraped_at"], "%Y-%m-%d").replace(
                    hour=12, tzinfo=timezone.utc
                )
                ts = dt.timestamp()
                if ts < prom_cutoff:  # only add pre-Prometheus data
                    merged[profile][ts] = row["physical_bytes"]

    # ── Prometheus: recent 30d ───────────────────────────────────────────────
    prom_start = max(now - days * 86400, prom_cutoff)
    prom_step = "6h" if days <= 30 else "1d"
    try:
        results = await prom_range(
            'export_storage_size_bytes{type="physical"}',
            str(int(prom_start)), str(now), prom_step
        )
        for r in results:
            label = _profile_label(r["metric"])
            if label is None:
                continue
            if label not in merged:
                merged[label] = {}
            for ts, val in r["values"]:
                merged[label][float(ts)] = merged[label].get(float(ts), 0) + float(val)
    except Exception as e:
        log.warning(f"Prometheus range query failed: {e}")

    # ── Serialise ────────────────────────────────────────────────────────────
    output = {}
    for profile, ts_map in merged.items():
        output[profile] = sorted(
            [{"ts": ts, "bytes": b} for ts, b in ts_map.items()],
            key=lambda x: x["ts"]
        )

    return output


@app.get("/api/apps/{profile_encoded}")
async def apps_breakdown(profile_encoded: str):
    """Per-namespace/app breakdown for a given profile (current snapshot)."""
    physical = await prom_query('export_storage_size_bytes{type="physical"}')

    result = []
    for r in physical:
        m = r["metric"]
        if _profile_label(m) != profile_encoded:
            continue
        ns = m.get("namespace", "unknown")
        rp = m.get("repo_path", "")
        app_name = rp.rstrip("/").replace("/kopia", "")
        if app_name.startswith("repo/"):
            app_name = ns
        elif app_name.startswith("export-"):
            app_name = f"export:{ns}"
        result.append({
            "app": app_name,
            "namespace": ns,
            "bytes": float(r["value"][1]),
            "human": _bytes_to_human(float(r["value"][1])),
        })

    result.sort(key=lambda x: -x["bytes"])
    return result


@app.get("/api/summary")
async def summary():
    """Top-level summary: totals, growth metrics, oldest DB record."""
    profiles = await list_profiles()

    total_phys = sum(p["physical_bytes"] for p in profiles)
    total_log  = sum(p["logical_bytes"]  for p in profiles)

    now = int(datetime.now(timezone.utc).timestamp())
    week_ago = now - 7 * 86400

    week_results = await prom_range(
        'export_storage_size_bytes{type="physical"}',
        str(week_ago), str(now), "1d"
    )

    first_map: dict[str, float] = {}
    last_map:  dict[str, float] = {}
    for r in week_results:
        label = _profile_label(r["metric"])
        if label is None:
            continue
        vals = r["values"]
        if not vals:
            continue
        first_map[label] = first_map.get(label, 0) + float(vals[0][1])
        last_map[label]  = last_map.get(label,  0) + float(vals[-1][1])

    growth_per_profile = {}
    for p in profiles:
        name = p["profile"]
        f = first_map.get(name, p["physical_bytes"])
        l = last_map.get(name,  p["physical_bytes"])
        delta = l - f
        pct = round((delta / f * 100) if f > 0 else 0, 2)
        growth_per_profile[name] = {
            "delta_bytes": delta,
            "delta_human": _bytes_to_human(abs(delta)),
            "direction": "up" if delta >= 0 else "down",
            "pct_7d": pct,
        }

    # Oldest record in DB
    with db() as con:
        oldest = con.execute(
            "SELECT MIN(scraped_at) as oldest FROM daily_snapshots"
        ).fetchone()["oldest"]

    return {
        "total_physical_bytes":  total_phys,
        "total_logical_bytes":   total_log,
        "total_physical_human":  _bytes_to_human(total_phys),
        "total_logical_human":   _bytes_to_human(total_log),
        "overall_dedup_ratio":   round(total_log / total_phys, 3) if total_phys > 0 else 1.0,
        "profile_count":         len(profiles),
        "history_since":         oldest,
        "growth_7d":             growth_per_profile,
        "profiles":              profiles,
    }


@app.get("/api/forecast/{profile}")
async def forecast(profile: str, days_ahead: int = Query(default=90, ge=1, le=730)):
    """
    Linear regression forecast using all available history (SQLite + Prometheus).
    Uses up to 365 days of history for the regression for better accuracy.
    """
    hist = await history(days=365, step="1d")
    points_raw = hist.get(profile)
    if not points_raw or len(points_raw) < 3:
        raise HTTPException(400, "Insufficient history for forecast")

    xs = [p["ts"] for p in points_raw]
    ys = [p["bytes"] for p in points_raw]
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(ys)
    slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / \
            sum((x - x_mean) ** 2 for x in xs)
    intercept = y_mean - slope * x_mean

    now = int(datetime.now(timezone.utc).timestamp())
    future = []
    for i in range(1, days_ahead + 1):
        fut_ts = now + i * 86400
        future.append({"ts": fut_ts, "bytes": max(0, slope * fut_ts + intercept)})

    return {
        "profile":             profile,
        "historical":          points_raw,
        "forecast":            future,
        "daily_growth_bytes":  slope * 86400,
        "daily_growth_human":  _bytes_to_human(abs(slope * 86400)),
        "days_ahead":          days_ahead,
        "history_points_used": len(xs),
    }


@app.get("/api/db/stats")
async def db_stats():
    """DB health: row count, oldest/newest record, size on disk."""
    import os as _os
    with db() as con:
        total = con.execute("SELECT COUNT(*) FROM daily_snapshots").fetchone()[0]
        oldest = con.execute("SELECT MIN(scraped_at) FROM daily_snapshots").fetchone()[0]
        newest = con.execute("SELECT MAX(scraped_at) FROM daily_snapshots").fetchone()[0]
        profiles = con.execute("SELECT COUNT(DISTINCT profile) FROM daily_snapshots").fetchone()[0]
    size = _os.path.getsize(DB_PATH) if _os.path.exists(DB_PATH) else 0
    return {
        "total_rows": total,
        "oldest_record": oldest,
        "newest_record": newest,
        "distinct_profiles": profiles,
        "db_size_bytes": size,
        "db_size_human": _bytes_to_human(size),
    }


# ---------------------------------------------------------------------------
# Static frontend
# ---------------------------------------------------------------------------

app.mount("/", StaticFiles(directory="/app/frontend", html=True), name="frontend")
