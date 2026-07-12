"""
Guidewire Insurance — near real-time stream pipeline (Apache Beam / GCP Dataflow).

Reads from three Pub/Sub topics:
  pc-policy-events  → PolicyCenter
  cc-claim-events   → ClaimCenter
  bc-billing-events → BillingCenter

Per domain:
  1. Parse & validate — bad messages → DLQ topic
  2. Enrich — add ingested_at, pipeline_version
  3. Write raw events → BigQuery raw.<domain>_events  (partitioned by timestamp)
  4. 5-min tumbling window → aggregate → BigQuery enriched.<domain>_*_5min

Run locally (DirectRunner — no GCP needed):
  python pipeline.py --project dummy --runner DirectRunner --dry-run

Deploy to Dataflow:
  python pipeline.py \
    --project <proj> \
    --region us-central1 \
    --runner DataflowRunner \
    --temp-location gs://<bucket>/tmp \
    --service-account-email ssh-dataflow-pipeline@<proj>.iam.gserviceaccount.com
"""

import argparse
import json
import logging
from datetime import datetime, timezone

from typing import Any

try:
    import apache_beam as beam
    from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
    from apache_beam.transforms.window import FixedWindows
    _BEAM_AVAILABLE = True
except ImportError:
    _BEAM_AVAILABLE = False

    class _TaggedOutput:
        def __init__(self, tag: str, value: Any):
            self.tag = tag
            self.value = value

    class _PValue:
        TaggedOutput = _TaggedOutput

    class _DoFn:
        WindowParam = None
        def process(self, element: Any, *args: Any, **kwargs: Any):  # type: ignore[empty-body]
            ...

    class _Beam:
        DoFn    = _DoFn
        pvalue  = _PValue()

    beam = _Beam()  # type: ignore[assignment]
    BigQueryDisposition = None  # type: ignore[assignment]
    WriteToBigQuery     = None  # type: ignore[assignment]
    PipelineOptions     = None  # type: ignore[assignment]
    StandardOptions     = None  # type: ignore[assignment]
    FixedWindows        = None  # type: ignore[assignment]

from processing.alert_rules import (
    evaluate_billing_alerts,
    evaluate_claim_alerts,
    evaluate_policy_alerts,
)

logger = logging.getLogger(__name__)

WINDOW_SECS      = 300   # 5-minute tumbling window
PIPELINE_VERSION = "1.0"

# ── BigQuery table refs ────────────────────────────────────────────────────────

def bq(project: str, dataset: str, table: str) -> str:
    return f"{project}:{dataset}.{table}"


# ── Shared transforms ──────────────────────────────────────────────────────────

class ParseMessage(beam.DoFn):
    """Deserialise Pub/Sub bytes → dict. Malformed messages go to DLQ output."""

    def process(self, message, *args, **kwargs):
        try:
            yield json.loads(message.data.decode("utf-8"))
        except Exception as e:
            yield beam.pvalue.TaggedOutput("dlq", {
                "raw": str(message.data[:500]),
                "error": str(e),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })


class ValidateEvent(beam.DoFn):
    """
    Check required Guidewire envelope fields.
    Domain-specific required payload fields checked per domain.
    Invalid events routed to DLQ tagged output.
    """

    ENVELOPE_REQUIRED = {"event_id", "event_type", "source_system", "domain",
                         "public_id", "entity_type", "timestamp", "source"}

    DOMAIN_REQUIRED = {
        "policy":  {"policy_number", "line_of_business", "status"},
        "claim":   {"claim_number", "policy_number", "loss_type"},
        "billing": {"billing_account_number", "policy_number"},
    }

    def process(self, event, *args, **kwargs):
        missing = self.ENVELOPE_REQUIRED - event.keys()
        if missing:
            yield beam.pvalue.TaggedOutput("dlq", {
                **event, "_error": f"missing envelope fields: {missing}"
            })
            return

        domain = event.get("domain")
        if domain not in self.DOMAIN_REQUIRED:
            yield beam.pvalue.TaggedOutput("dlq", {
                **event, "_error": f"unknown domain: {domain}"
            })
            return

        payload        = event.get("payload") or {}
        payload_missing = self.DOMAIN_REQUIRED[domain] - payload.keys()
        if payload_missing:
            yield beam.pvalue.TaggedOutput("dlq", {
                **event, "_error": f"missing payload fields: {payload_missing}"
            })
            return

        yield event


class EnrichEvent(beam.DoFn):
    """Stamp pipeline metadata onto each event before writing to BigQuery."""

    def process(self, event, *args, **kwargs):
        yield {
            **event,
            "ingested_at":       datetime.now(timezone.utc).isoformat(),
            "_pipeline_version": PIPELINE_VERSION,
        }


class StripInternalFields(beam.DoFn):
    """Remove pipeline-internal underscore fields before writing to BQ."""

    def process(self, event, *args, **kwargs):
        yield {k: v for k, v in event.items() if not k.startswith("_")}


# ═══════════════════════════════════════════════════════════════════════════════
# PolicyCenter — windowed aggregation
# ═══════════════════════════════════════════════════════════════════════════════

class AggregatePolicyWindow(beam.DoFn):
    """
    Key: (event_type, line_of_business, state)
    Output: policy_summary_5min row
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        key, events = element
        event_type, lob, state = key
        events = list(events)

        total_premium     = sum(
            (e.get("payload") or {}).get("written_premium") or 0
            for e in events
        )
        cancellation_count = sum(
            1 for e in events if e.get("event_type") == "policy.cancelled"
        )

        yield {
            "window_start":         window.start.to_utc_datetime().isoformat(),
            "window_end":           window.end.to_utc_datetime().isoformat(),
            "event_type":           event_type,
            "line_of_business":     lob,
            "state":                state,
            "event_count":          len(events),
            "total_written_premium": total_premium,
            "cancellation_count":   cancellation_count,
            "computed_at":          datetime.now(timezone.utc).isoformat(),
        }


def _policy_key(event: dict) -> tuple:
    p = event.get("payload") or {}
    return (
        event.get("event_type", "unknown"),
        p.get("line_of_business", "unknown"),
        p.get("state", "unknown"),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ClaimCenter — windowed aggregation
# ═══════════════════════════════════════════════════════════════════════════════

class AggregateClaimWindow(beam.DoFn):
    """
    Key: (event_type, line_of_business)
    Output: claim_activity_5min row
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        key, events = element
        event_type, lob = key
        events = list(events)

        total_reserves = sum(
            (e.get("payload") or {}).get("reserve_amount") or 0
            for e in events
        )
        total_paid = sum(
            (e.get("payload") or {}).get("paid_to_date") or 0
            for e in events
        )
        cat_count = sum(
            1 for e in events
            if (e.get("payload") or {}).get("catastrophe_code") is not None
        )

        yield {
            "window_start":     window.start.to_utc_datetime().isoformat(),
            "window_end":       window.end.to_utc_datetime().isoformat(),
            "event_type":       event_type,
            "line_of_business": lob,
            "event_count":      len(events),
            "total_reserves":   total_reserves,
            "total_paid":       total_paid,
            "cat_event_count":  cat_count,
            "computed_at":      datetime.now(timezone.utc).isoformat(),
        }


def _claim_key(event: dict) -> tuple:
    p = event.get("payload") or {}
    return (
        event.get("event_type", "unknown"),
        p.get("line_of_business", "unknown"),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# BillingCenter — windowed aggregation
# ═══════════════════════════════════════════════════════════════════════════════

class AggregateBillingWindow(beam.DoFn):
    """
    Key: event_type
    Output: billing_summary_5min row
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        event_type, events = element
        events = list(events)

        total_collected = sum(
            (e.get("payload") or {}).get("amount_paid") or 0
            for e in events
        )
        total_due = sum(
            (e.get("payload") or {}).get("amount_due") or 0
            for e in events
        )
        delinquency_count = sum(
            1 for e in events if e.get("event_type") in (
                "delinquency.opened", "delinquency.closed"
            )
        )
        failure_count = sum(
            1 for e in events if e.get("event_type") in (
                "payment.failed", "payment.reversed"
            )
        )

        yield {
            "window_start":           window.start.to_utc_datetime().isoformat(),
            "window_end":             window.end.to_utc_datetime().isoformat(),
            "event_type":             event_type,
            "event_count":            len(events),
            "total_amount_collected": total_collected,
            "total_amount_due":       total_due,
            "delinquency_count":      delinquency_count,
            "payment_failure_count":  failure_count,
            "computed_at":            datetime.now(timezone.utc).isoformat(),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline builder
# ═══════════════════════════════════════════════════════════════════════════════

class DetectAlerts(beam.DoFn):
    """
    Receives a windowed aggregate row, evaluates alert rules, emits alert dicts.
    domain: "policy" | "claim" | "billing"
    """

    EVALUATORS = {
        "policy":  evaluate_policy_alerts,
        "claim":   evaluate_claim_alerts,
        "billing": evaluate_billing_alerts,
    }

    def __init__(self, domain: str):
        self.domain = domain

    def process(self, agg: dict[str, Any], *args, **kwargs):
        evaluator = self.EVALUATORS.get(self.domain)
        if evaluator:
            for alert in evaluator(agg):
                yield alert


def build_pipeline(project: str, options: PipelineOptions):
    # Pub/Sub topics
    pc_topic      = f"projects/{project}/topics/pc-policy-events"
    cc_topic      = f"projects/{project}/topics/cc-claim-events"
    bc_topic      = f"projects/{project}/topics/bc-billing-events"
    alerts_topic  = f"projects/{project}/topics/insurance-alerts"

    # BigQuery raw tables
    bq_raw_policy   = bq(project, "raw", "policy_events")
    bq_raw_claim    = bq(project, "raw", "claim_events")
    bq_raw_billing  = bq(project, "raw", "billing_events")
    bq_raw_alerts   = bq(project, "raw", "alerts")

    # BigQuery enriched tables
    bq_enr_policy   = bq(project, "enriched", "policy_summary_5min")
    bq_enr_claim    = bq(project, "enriched", "claim_activity_5min")
    bq_enr_billing  = bq(project, "enriched", "billing_summary_5min")

    write_cfg: dict[str, Any] = dict(
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
    )

    with beam.Pipeline(options=options) as p:

        # ── Ingest ─────────────────────────────────────────────────────────────
        pc_raw = p | "PC_Read"  >> beam.io.ReadFromPubSub(topic=pc_topic, with_attributes=True)
        cc_raw = p | "CC_Read"  >> beam.io.ReadFromPubSub(topic=cc_topic, with_attributes=True)
        bc_raw = p | "BC_Read"  >> beam.io.ReadFromPubSub(topic=bc_topic, with_attributes=True)

        # ── Parse ──────────────────────────────────────────────────────────────
        pc_parsed = pc_raw | "PC_Parse" >> beam.ParDo(ParseMessage()).with_outputs("dlq", main="ok")
        cc_parsed = cc_raw | "CC_Parse" >> beam.ParDo(ParseMessage()).with_outputs("dlq", main="ok")
        bc_parsed = bc_raw | "BC_Parse" >> beam.ParDo(ParseMessage()).with_outputs("dlq", main="ok")

        # ── Validate ───────────────────────────────────────────────────────────
        pc_valid = pc_parsed.ok | "PC_Validate" >> beam.ParDo(ValidateEvent()).with_outputs("dlq", main="ok")
        cc_valid = cc_parsed.ok | "CC_Validate" >> beam.ParDo(ValidateEvent()).with_outputs("dlq", main="ok")
        bc_valid = bc_parsed.ok | "BC_Validate" >> beam.ParDo(ValidateEvent()).with_outputs("dlq", main="ok")

        # ── Enrich ─────────────────────────────────────────────────────────────
        pc_enriched = pc_valid.ok | "PC_Enrich" >> beam.ParDo(EnrichEvent())
        cc_enriched = cc_valid.ok | "CC_Enrich" >> beam.ParDo(EnrichEvent())
        bc_enriched = bc_valid.ok | "BC_Enrich" >> beam.ParDo(EnrichEvent())

        # ── Strip internal fields before BQ write ──────────────────────────────
        pc_clean = pc_enriched | "PC_Strip" >> beam.ParDo(StripInternalFields())
        cc_clean = cc_enriched | "CC_Strip" >> beam.ParDo(StripInternalFields())
        bc_clean = bc_enriched | "BC_Strip" >> beam.ParDo(StripInternalFields())

        # ── Write raw events → BigQuery ────────────────────────────────────────
        pc_clean | "PC_WriteRaw" >> WriteToBigQuery(bq_raw_policy,  **write_cfg)
        cc_clean | "CC_WriteRaw" >> WriteToBigQuery(bq_raw_claim,   **write_cfg)
        bc_clean | "BC_WriteRaw" >> WriteToBigQuery(bq_raw_billing, **write_cfg)

        # ── PolicyCenter: 5-min window → aggregate → BQ + alerts ─────────────────
        pc_agg = (
            pc_enriched
            | "PC_Window" >> beam.WindowInto(FixedWindows(WINDOW_SECS))
            | "PC_KV"     >> beam.Map(lambda e: (_policy_key(e), e))
            | "PC_Group"  >> beam.GroupByKey()
            | "PC_Agg"    >> beam.ParDo(AggregatePolicyWindow())
        )
        pc_agg | "PC_WriteAgg"   >> WriteToBigQuery(bq_enr_policy, **write_cfg)
        pc_agg | "PC_Alerts"     >> beam.ParDo(DetectAlerts("policy")) \
               | "PC_WriteAlerts">> WriteToBigQuery(bq_raw_alerts, **write_cfg)

        # ── ClaimCenter: 5-min window → aggregate → BQ + alerts ───────────────
        cc_agg = (
            cc_enriched
            | "CC_Window" >> beam.WindowInto(FixedWindows(WINDOW_SECS))
            | "CC_KV"     >> beam.Map(lambda e: (_claim_key(e), e))
            | "CC_Group"  >> beam.GroupByKey()
            | "CC_Agg"    >> beam.ParDo(AggregateClaimWindow())
        )
        cc_agg | "CC_WriteAgg"   >> WriteToBigQuery(bq_enr_claim, **write_cfg)
        cc_agg | "CC_Alerts"     >> beam.ParDo(DetectAlerts("claim")) \
               | "CC_WriteAlerts">> WriteToBigQuery(bq_raw_alerts, **write_cfg)

        # ── BillingCenter: 5-min window → aggregate → BQ + alerts ────────────────
        bc_agg = (
            bc_enriched
            | "BC_Window" >> beam.WindowInto(FixedWindows(WINDOW_SECS))
            | "BC_KV"     >> beam.Map(lambda e: (e.get("event_type", "unknown"), e))
            | "BC_Group"  >> beam.GroupByKey()
            | "BC_Agg"    >> beam.ParDo(AggregateBillingWindow())
        )
        bc_agg | "BC_WriteAgg"   >> WriteToBigQuery(bq_enr_billing, **write_cfg)
        bc_agg | "BC_Alerts"     >> beam.ParDo(DetectAlerts("billing")) \
               | "BC_WriteAlerts">> WriteToBigQuery(bq_raw_alerts, **write_cfg)


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Guidewire insurance stream pipeline")
    parser.add_argument("--project",  required=True, help="GCP project ID")
    parser.add_argument("--region",   default="us-central1")
    parser.add_argument("--runner",   default="DirectRunner",
                        choices=["DirectRunner", "DataflowRunner"])
    parser.add_argument("--temp-location", default=None,
                        help="GCS path for Dataflow temp, e.g. gs://bucket/tmp")
    parser.add_argument("--service-account-email", default=None,
                        help="Dataflow SA email (DataflowRunner only)")
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(
        beam_args,
        project=args.project,
        region=args.region,
        runner=args.runner,
        streaming=True,
        save_main_session=True,
        temp_location=args.temp_location,
        service_account_email=args.service_account_email,
    )
    options.view_as(StandardOptions).streaming = True

    build_pipeline(args.project, options)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
