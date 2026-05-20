#!/usr/bin/env python3
"""
Kasten Metrics Model - Daily Scraper
Runs as a CronJob. Pulls current profile totals from Prometheus,
appends to SQLite, purges rows older than 5 years.
"""

import os
import sys
import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta

import httpx
from kubernetes import client, config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("scraper")

PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL",
    "http://prometheus-server.kasten-io.svc/k10/prometheus"
)
KASTEN_NAMESPACE = os.getenv("KASTEN_NAMESPACE", "kasten-io")
DB_PATH = os.getenv("DB_PATH", "/data/metrics.db")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "1825"))  # 5 years

# VBR config — optional, skipped if not set
VBR_URL      = os.getenv("VBR_URL", "")           # e.g. https://192.168.1.153:9419
VBR_USER     = os.getenv("VBR_USER", "")
VBR_PASSWORD = os.getenv("VBR_PASSWORD", "")
# Comma-separated repo_name=kasten_profile pairs e.g. "VBR=vbr,SOBR=vbr-sobr"
VBR_REPO_MAP = os.getenv("VBR_REPO_MAP", "VBR=vbr,SOBR=vbr-sobr")


# ---------------------------------------------------------------------------
# DB
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
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                scraped_at        TEXT NOT NULL,
                profile           TEXT NOT NULL,
                physical_bytes    REAL NOT NULL,
                logical_bytes     REAL NOT NULL,
                object_store_type TEXT,
                endpoint          TEXT,
                bucket            TEXT
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_profile_date
            ON daily_snapshots (profile, scraped_at)
        """)
        con.commit()


def already_scraped_today(today: str) -> bool:
    with db() as con:
        row = con.execute(
            "SELECT 1 FROM daily_snapshots WHERE scraped_at = ? LIMIT 1",
            (today,)
        ).fetchone()
    return row is not None


def insert_rows(today: str, rows: list[dict]):
    with db() as con:
        con.executemany("""
            INSERT INTO daily_snapshots
              (scraped_at, profile, physical_bytes, logical_bytes,
               object_store_type, endpoint, bucket)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                today,
                r["profile"],
                r["physical_bytes"],
                r["logical_bytes"],
                r.get("object_store_type", ""),
                r.get("endpoint", ""),
                r.get("bucket", ""),
            )
            for r in rows
        ])
        con.commit()
    log.info(f"Inserted {len(rows)} rows for {today}")


def purge_old_rows():
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    with db() as con:
        deleted = con.execute(
            "DELETE FROM daily_snapshots WHERE scraped_at < ?", (cutoff,)
        ).rowcount
        con.commit()
    if deleted:
        log.info(f"Purged {deleted} rows older than {cutoff} ({RETENTION_DAYS}d retention)")


# ---------------------------------------------------------------------------
# Kubernetes profile map
# ---------------------------------------------------------------------------

def _k8s_api():
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client.CustomObjectsApi()


def load_profile_map() -> dict:
    profiles = {}
    try:
        api = _k8s_api()
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
        log.warning(f"CRD profile load failed: {e}")
    return profiles


def load_policy_profile_map() -> dict:
    result = {}
    try:
        api = _k8s_api()
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
        log.info(f"Loaded {len(result)} policy->profile mappings")
    except Exception as e:
        log.warning(f"Policy->profile map load failed: {e}")
    return result


# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------

def prom_query(query: str) -> list:
    r = httpx.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": query},
        timeout=30
    )
    r.raise_for_status()
    data = r.json()
    if data["status"] != "success":
        raise RuntimeError(f"Prometheus error: {data}")
    return data["data"]["result"]


def profile_label(m: dict, profile_map: dict, policy_map: dict) -> str:
    ost    = m.get("object_store_type", "")
    ep     = m.get("endpoint", "")
    bucket = m.get("bucket", "")
    repo_path = m.get("repo_path", "")

    if ost or ep or bucket:
        key = (ost, ep, bucket)
        if key in profile_map:
            return profile_map[key]
        host = ep.replace("https://", "").replace("http://", "").rstrip("/")
        if host and bucket:
            return f"{host}/{bucket}"
        return bucket or ep or ost

    rp = repo_path.rstrip("/")
    if rp.endswith("/kopia"):
        rp = rp[:-len("/kopia")]
    if rp.startswith("repo/"):
        return None
    if rp.startswith("export-"):
        return "manual-exports"
    if rp in policy_map:
        return policy_map[rp]
    return rp or "unknown"



# ---------------------------------------------------------------------------
# VBR helpers
# ---------------------------------------------------------------------------

def vbr_token() -> str:
    """Obtain OAuth2 bearer token from VBR REST API."""
    r = httpx.post(
        f"{VBR_URL}/api/oauth2/token",
        data={
            "grant_type": "password",
            "username": VBR_USER,
            "password": VBR_PASSWORD,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False,
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def vbr_repo_states(token: str) -> list[dict]:
    """Fetch all repository states from VBR."""
    r = httpx.get(
        f"{VBR_URL}/api/v1/backupInfrastructure/repositories/states",
        params={"limit": 100},
        headers={"Authorization": f"Bearer {token}"},
        verify=False,
        timeout=15,
    )
    r.raise_for_status()
    return r.json().get("data", [])


def vbr_scrape() -> list[dict]:
    """
    Returns rows for VBR-backed Kasten profiles.
    Maps repository names and SOBR extents to Kasten profile names via VBR_REPO_MAP.
    """
    if not VBR_URL or not VBR_USER or not VBR_PASSWORD:
        log.info("VBR not configured, skipping")
        return []

    # Parse repo map: "VBR=vbr,SOBR=vbr-sobr" -> {"VBR": "vbr", "SOBR": "vbr-sobr"}
    repo_map = {}
    for pair in VBR_REPO_MAP.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            repo_map[k.strip()] = v.strip()

    try:
        token = vbr_token()
        states = vbr_repo_states(token)
    except Exception as e:
        log.warning(f"VBR scrape failed: {e}")
        return []

    # Build two maps: direct repos and SOBR extent aggregation
    # direct: repo_name -> usedSpaceGB (for non-SOBR repos)
    # sobr_extents: scaleOutRepositoryId -> {name, usedGB}
    direct: dict[str, float] = {}
    sobr_totals: dict[str, float] = {}   # sobr_id -> total used GB
    sobr_names: dict[str, str] = {}      # sobr_id -> SOBR name (from scaleOutRepositories)

    for repo in states:
        name = repo.get("name", "")
        used = repo.get("usedSpaceGB", 0.0)
        sobr_details = repo.get("scaleOutRepositoryDetails")
        if sobr_details:
            sobr_id = sobr_details.get("scaleOutRepositoryId", "")
            sobr_totals[sobr_id] = sobr_totals.get(sobr_id, 0.0) + used
        else:
            direct[name] = used

    # Fetch SOBR names
    try:
        r = httpx.get(
            f"{VBR_URL}/api/v1/backupInfrastructure/scaleOutRepositories",
            params={"limit": 100},
            headers={"Authorization": f"Bearer {token}"},
            verify=False,
            timeout=15,
        )
        r.raise_for_status()
        for sobr in r.json().get("data", []):
            sobr_names[sobr["id"]] = sobr["name"]
    except Exception as e:
        log.warning(f"SOBR list failed: {e}")

    rows = []
    # Direct repos
    for repo_name, kasten_profile in repo_map.items():
        if repo_name in direct:
            used_bytes = direct[repo_name] * 1024 ** 3
            rows.append({
                "profile":           kasten_profile,
                "physical_bytes":    used_bytes,
                "logical_bytes":     used_bytes,  # VBR doesn't expose dedup ratio
                "object_store_type": "VBR",
                "endpoint":          VBR_URL,
                "bucket":            repo_name,
            })
            log.info(f"VBR repo {repo_name} -> {kasten_profile}: {direct[repo_name]:.1f} GB")

    # SOBR repos
    for sobr_id, total_gb in sobr_totals.items():
        sobr_name = sobr_names.get(sobr_id, sobr_id)
        if sobr_name in repo_map:
            kasten_profile = repo_map[sobr_name]
            used_bytes = total_gb * 1024 ** 3
            rows.append({
                "profile":           kasten_profile,
                "physical_bytes":    used_bytes,
                "logical_bytes":     used_bytes,
                "object_store_type": "VBR-SOBR",
                "endpoint":          VBR_URL,
                "bucket":            sobr_name,
            })
            log.info(f"VBR SOBR {sobr_name} -> {kasten_profile}: {total_gb:.1f} GB")

    return rows

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log.info(f"Scraper starting — date={today}, db={DB_PATH}")

    init_db()

    if already_scraped_today(today):
        log.info(f"Already have data for {today}, skipping")
        purge_old_rows()
        return

    profile_map = load_profile_map()
    policy_map = load_policy_profile_map()

    # Query physical and logical
    try:
        physical = prom_query('export_storage_size_bytes{type="physical"}')
        logical  = prom_query('export_storage_size_bytes{type="logical"}')
    except Exception as e:
        log.error(f"Prometheus query failed: {e}")
        sys.exit(1)

    # Aggregate by profile
    phys_map: dict[str, float] = {}
    meta_map: dict[str, dict]  = {}
    for r in physical:
        m = r["metric"]
        label = profile_label(m, profile_map, policy_map)
        if label is None:
            continue
        phys_map[label] = phys_map.get(label, 0) + float(r["value"][1])
        if label not in meta_map:
            meta_map[label] = {
                "object_store_type": m.get("object_store_type", ""),
                "endpoint":          m.get("endpoint", ""),
                "bucket":            m.get("bucket", ""),
            }

    log_map: dict[str, float] = {}
    for r in logical:
        m = r["metric"]
        label = profile_label(m, profile_map, policy_map)
        if label is None:
            continue
        log_map[label] = log_map.get(label, 0) + float(r["value"][1])

    rows = []
    for profile, phys in phys_map.items():
        if phys == 0:
            continue
        meta = meta_map.get(profile, {})
        rows.append({
            "profile":           profile,
            "physical_bytes":    phys,
            "logical_bytes":     log_map.get(profile, 0),
            "object_store_type": meta.get("object_store_type", ""),
            "endpoint":          meta.get("endpoint", ""),
            "bucket":            meta.get("bucket", ""),
        })

    # Add VBR rows
    vbr_rows = vbr_scrape()
    rows.extend(vbr_rows)

    if not rows:
        log.warning("No rows to insert — Prometheus and VBR returned no data")
        sys.exit(1)

    insert_rows(today, rows)
    purge_old_rows()
    log.info("Scraper complete")


if __name__ == "__main__":
    main()
