"""
SalesServiceHub REST API — serves Guidewire insurance data products from BigQuery.

Endpoints:
  GET /health
  GET /policies/active          active_policy_snapshot
  GET /policies/performance     policy_performance (rolling 24h)
  GET /claims/open              open_claims_summary
  GET /claims/exposure          claims_exposure (rolling 24h)
  GET /billing/health           billing_health (rolling 24h)
  GET /billing/delinquencies    delinquency_watchlist
  GET /alerts                   recent alerts from raw.alerts

Run locally:
  uvicorn serving.api:app --reload --port 8080

Environment variables:
  GCP_PROJECT   GCP project ID (required)
  BQ_LOCATION   BigQuery location (default: US)
  API_PAGE_SIZE Max rows per response (default: 500)
"""

import os
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

try:
    from google.cloud import bigquery
    _bq_available = True
except ImportError:
    _bq_available = False

app = FastAPI(
    title="SalesServiceHub — Guidewire Insurance Data Products API",
    version="1.0.0",
    description="Near real-time insurance data products from PolicyCenter, ClaimCenter, BillingCenter",
)

PROJECT    = os.environ.get("GCP_PROJECT", "")
BQ_LOC     = os.environ.get("BQ_LOCATION", "US")
PAGE_SIZE  = int(os.environ.get("API_PAGE_SIZE", "500"))


def _client() -> "bigquery.Client":
    if not _bq_available:
        raise HTTPException(status_code=503, detail="google-cloud-bigquery not installed")
    if not PROJECT:
        raise HTTPException(status_code=503, detail="GCP_PROJECT env var not set")
    return bigquery.Client(project=PROJECT, location=BQ_LOC)


def _run(sql: str, params: Optional[list] = None) -> list[dict[str, Any]]:
    client = _client()
    job    = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params or []))
    return [dict(row) for row in job.result()]


def _table(dataset: str, table: str) -> str:
    return f"`{PROJECT}.{dataset}.{table}`"


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok", "project": PROJECT}


# ── PolicyCenter ───────────────────────────────────────────────────────────────

@app.get("/policies/active", tags=["policy"])
def active_policies(
    lob:    Optional[str] = Query(None, description="Filter by line_of_business"),
    state:  Optional[str] = Query(None, description="Filter by state (2-letter)"),
    limit:  int           = Query(PAGE_SIZE, le=PAGE_SIZE),
):
    """Latest active policy per policy_number from PolicyCenter raw events."""
    where_clauses = ["1=1"]
    params: list = []

    if lob:
        where_clauses.append("line_of_business = @lob")
        params.append(bigquery.ScalarQueryParameter("lob", "STRING", lob))
    if state:
        where_clauses.append("state = @state")
        params.append(bigquery.ScalarQueryParameter("state", "STRING", state))

    where = " AND ".join(where_clauses)
    sql = f"""
        SELECT * FROM {_table("data_products", "active_policy_snapshot")}
        WHERE {where}
        ORDER BY latest_event_time DESC
        LIMIT @limit
    """
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    return _run(sql, params)


@app.get("/policies/performance", tags=["policy"])
def policy_performance(
    lob:   Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    hours: int           = Query(24, ge=1, le=24, description="Lookback hours (max 24)"),
):
    """5-min windowed policy performance metrics."""
    where_clauses = ["window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)"]
    params = [bigquery.ScalarQueryParameter("hours", "INT64", hours)]

    if lob:
        where_clauses.append("line_of_business = @lob")
        params.append(bigquery.ScalarQueryParameter("lob", "STRING", lob))
    if state:
        where_clauses.append("state = @state")
        params.append(bigquery.ScalarQueryParameter("state", "STRING", state))

    sql = f"""
        SELECT * FROM {_table("data_products", "policy_performance")}
        WHERE {" AND ".join(where_clauses)}
        ORDER BY window_start DESC
    """
    return _run(sql, params)


# ── ClaimCenter ────────────────────────────────────────────────────────────────

@app.get("/claims/open", tags=["claim"])
def open_claims(
    lob:      Optional[str] = Query(None),
    adjuster: Optional[str] = Query(None, description="Filter by assigned_adjuster"),
    cat_only: bool          = Query(False, description="Return only CAT-flagged claims"),
    limit:    int           = Query(PAGE_SIZE, le=PAGE_SIZE),
):
    """All currently open claims from ClaimCenter raw events."""
    where_clauses = ["1=1"]
    params: list = []

    if lob:
        where_clauses.append("line_of_business = @lob")
        params.append(bigquery.ScalarQueryParameter("lob", "STRING", lob))
    if adjuster:
        where_clauses.append("assigned_adjuster = @adjuster")
        params.append(bigquery.ScalarQueryParameter("adjuster", "STRING", adjuster))
    if cat_only:
        where_clauses.append("catastrophe_code IS NOT NULL")

    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    sql = f"""
        SELECT * FROM {_table("data_products", "open_claims_summary")}
        WHERE {" AND ".join(where_clauses)}
        ORDER BY open_reserve DESC
        LIMIT @limit
    """
    return _run(sql, params)


@app.get("/claims/exposure", tags=["claim"])
def claims_exposure(
    lob:   Optional[str] = Query(None),
    hours: int           = Query(24, ge=1, le=24),
):
    """5-min windowed claims exposure metrics."""
    where_clauses = ["window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)"]
    params = [bigquery.ScalarQueryParameter("hours", "INT64", hours)]

    if lob:
        where_clauses.append("line_of_business = @lob")
        params.append(bigquery.ScalarQueryParameter("lob", "STRING", lob))

    sql = f"""
        SELECT * FROM {_table("data_products", "claims_exposure")}
        WHERE {" AND ".join(where_clauses)}
        ORDER BY window_start DESC
    """
    return _run(sql, params)


# ── BillingCenter ──────────────────────────────────────────────────────────────

@app.get("/billing/health", tags=["billing"])
def billing_health(hours: int = Query(24, ge=1, le=24)):
    """5-min windowed billing health metrics: collection rate, failure rate."""
    sql = f"""
        SELECT * FROM {_table("data_products", "billing_health")}
        WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
        ORDER BY window_start DESC
    """
    params = [bigquery.ScalarQueryParameter("hours", "INT64", hours)]
    return _run(sql, params)


@app.get("/billing/delinquencies", tags=["billing"])
def delinquency_watchlist(
    risk_tier: Optional[str] = Query(None,
        description="CriticalRisk | HighRisk | MediumRisk | LowRisk"),
    plan:      Optional[str] = Query(None, description="Filter by payment_plan"),
    limit:     int           = Query(PAGE_SIZE, le=PAGE_SIZE),
):
    """Active delinquent billing accounts ranked by risk tier."""
    where_clauses = ["1=1"]
    params: list = []

    if risk_tier:
        where_clauses.append("risk_tier = @risk_tier")
        params.append(bigquery.ScalarQueryParameter("risk_tier", "STRING", risk_tier))
    if plan:
        where_clauses.append("payment_plan = @plan")
        params.append(bigquery.ScalarQueryParameter("plan", "STRING", plan))

    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    sql = f"""
        SELECT * FROM {_table("data_products", "delinquency_watchlist")}
        WHERE {" AND ".join(where_clauses)}
        ORDER BY days_past_due DESC, outstanding_balance DESC
        LIMIT @limit
    """
    return _run(sql, params)


# ── Alerts ─────────────────────────────────────────────────────────────────────

@app.get("/alerts", tags=["ops"])
def recent_alerts(
    domain:   Optional[str] = Query(None, description="policy | claim | billing"),
    severity: Optional[str] = Query(None, description="critical | high | medium | low"),
    hours:    int           = Query(1, ge=1, le=24),
    limit:    int           = Query(100, le=500),
):
    """Recent pipeline-generated alerts across all three Guidewire domains."""
    where_clauses = ["triggered_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)"]
    params = [bigquery.ScalarQueryParameter("hours", "INT64", hours)]

    if domain:
        where_clauses.append("domain = @domain")
        params.append(bigquery.ScalarQueryParameter("domain", "STRING", domain))
    if severity:
        where_clauses.append("severity = @severity")
        params.append(bigquery.ScalarQueryParameter("severity", "STRING", severity))

    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    sql = f"""
        SELECT * FROM {_table("raw", "alerts")}
        WHERE {" AND ".join(where_clauses)}
        ORDER BY triggered_at DESC
        LIMIT @limit
    """
    return _run(sql, params)
