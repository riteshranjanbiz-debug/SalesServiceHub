"""
Unit tests for Beam pipeline transforms.
DoFns are tested by calling .process() directly — no Beam runtime required.
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from processing.pipeline import (
    ParseMessage,
    ValidateEvent,
    EnrichEvent,
    StripInternalFields,
    AggregatePolicyWindow,
    AggregateClaimWindow,
    AggregateBillingWindow,
    _policy_key,
    _claim_key,
    _TaggedOutput,
)
from processing.alert_rules import evaluate_policy_alerts, evaluate_claim_alerts, evaluate_billing_alerts


# ── Helpers ────────────────────────────────────────────────────────────────────

class FakeMessage:
    def __init__(self, data: dict):
        self.data = json.dumps(data).encode("utf-8")


class FakeWindow:
    class _Ts:
        def to_utc_datetime(self):
            from datetime import datetime, timezone
            return datetime(2026, 7, 12, 10, 0, 0, tzinfo=timezone.utc)
    start = _Ts()
    end   = _Ts()


def collect(dofn, *args, **kwargs):
    """Run a DoFn.process() and collect all non-tagged outputs."""
    return list(dofn.process(*args, **kwargs))


def _policy_event(**overrides) -> dict:
    base = {
        "event_id": "evt-001", "event_type": "policy.bound",
        "source_system": "guidewire_pc", "domain": "policy",
        "public_id": "pc:Policy:1", "entity_type": "Policy",
        "timestamp": "2026-07-12T10:00:00+00:00", "source": "ui",
        "payload": {
            "policy_number": "PC-2026-001", "line_of_business": "PersonalAutoLine",
            "status": "Bound", "written_premium": 1200.0, "state": "CA",
        },
    }
    base.update(overrides)
    return base


def _claim_event(**overrides) -> dict:
    base = {
        "event_id": "evt-002", "event_type": "claim.assigned",
        "source_system": "guidewire_cc", "domain": "claim",
        "public_id": "cc:Claim:1", "entity_type": "Claim",
        "timestamp": "2026-07-12T10:00:00+00:00", "source": "api",
        "payload": {
            "claim_number": "CL-2026-001", "policy_number": "PC-2026-001",
            "loss_type": "collision", "reserve_amount": 5000.0,
            "paid_to_date": 0.0, "status": "Open",
            "line_of_business": "PersonalAutoLine",
            "subrogation_flag": False, "litigation_flag": False,
        },
    }
    base.update(overrides)
    return base


def _billing_event(**overrides) -> dict:
    base = {
        "event_id": "evt-003", "event_type": "payment.received",
        "source_system": "guidewire_bc", "domain": "billing",
        "public_id": "bc:Account:1", "entity_type": "Payment",
        "timestamp": "2026-07-12T10:00:00+00:00", "source": "payment_portal",
        "payload": {
            "billing_account_number": "BC-001", "policy_number": "PC-2026-001",
            "amount_due": 100.0, "amount_paid": 100.0,
            "outstanding_balance": 0.0, "payment_plan": "Monthly",
            "days_past_due": 0,
        },
    }
    base.update(overrides)
    return base


# ── ParseMessage ───────────────────────────────────────────────────────────────

class TestParseMessage:
    def test_valid_json_passes_through(self):
        event = _policy_event()
        result = collect(ParseMessage(), FakeMessage(event))
        assert len(result) == 1
        assert result[0]["event_id"] == "evt-001"

    def test_invalid_json_goes_to_dlq(self):
        class BadMsg:
            data = b"not json {"

        results = list(ParseMessage().process(BadMsg()))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert len(tagged) == 1
        assert tagged[0].tag == "dlq"

    def test_preserves_all_fields(self):
        event = _claim_event()
        result = collect(ParseMessage(), FakeMessage(event))
        assert result[0]["source_system"] == "guidewire_cc"
        assert result[0]["payload"]["claim_number"] == "CL-2026-001"


# ── ValidateEvent ──────────────────────────────────────────────────────────────

class TestValidateEvent:
    def test_valid_policy_event_passes(self):
        result = collect(ValidateEvent(), _policy_event())
        assert len(result) == 1

    def test_valid_claim_event_passes(self):
        result = collect(ValidateEvent(), _claim_event())
        assert len(result) == 1

    def test_valid_billing_event_passes(self):
        result = collect(ValidateEvent(), _billing_event())
        assert len(result) == 1

    def test_missing_envelope_field_goes_to_dlq(self):
        bad = _policy_event()
        del bad["event_id"]
        results = list(ValidateEvent().process(bad))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert tagged[0].tag == "dlq"
        assert "missing envelope fields" in tagged[0].value["_error"]

    def test_unknown_domain_goes_to_dlq(self):
        bad = _policy_event(domain="unknown_system")
        results = list(ValidateEvent().process(bad))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert tagged[0].tag == "dlq"

    def test_missing_payload_field_goes_to_dlq(self):
        bad = _claim_event()
        del bad["payload"]["claim_number"]
        results = list(ValidateEvent().process(bad))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert tagged[0].tag == "dlq"


# ── EnrichEvent ────────────────────────────────────────────────────────────────

class TestEnrichEvent:
    def test_adds_ingested_at(self):
        result = collect(EnrichEvent(), _policy_event())
        assert "ingested_at" in result[0]
        assert result[0]["ingested_at"] is not None

    def test_adds_pipeline_version(self):
        result = collect(EnrichEvent(), _claim_event())
        assert result[0]["_pipeline_version"] == "1.0"

    def test_original_fields_preserved(self):
        result = collect(EnrichEvent(), _billing_event())
        assert result[0]["event_id"] == "evt-003"
        assert result[0]["payload"]["amount_paid"] == 100.0


# ── StripInternalFields ────────────────────────────────────────────────────────

class TestStripInternalFields:
    def test_removes_underscore_fields(self):
        enriched = {**_policy_event(), "_pipeline_version": "1.0", "_error": "x"}
        result = collect(StripInternalFields(), enriched)
        assert "_pipeline_version" not in result[0]
        assert "_error" not in result[0]

    def test_keeps_public_fields(self):
        enriched = {**_policy_event(), "_pipeline_version": "1.0", "ingested_at": "ts"}
        result = collect(StripInternalFields(), enriched)
        assert result[0]["event_id"] == "evt-001"
        assert result[0]["ingested_at"] == "ts"


# ── Aggregation key functions ──────────────────────────────────────────────────

class TestKeyFunctions:
    def test_policy_key_extracts_tuple(self):
        key = _policy_key(_policy_event())
        assert key == ("policy.bound", "PersonalAutoLine", "CA")

    def test_claim_key_extracts_tuple(self):
        key = _claim_key(_claim_event())
        assert key == ("claim.assigned", "PersonalAutoLine")

    def test_policy_key_handles_missing_payload(self):
        event = _policy_event()
        del event["payload"]
        key = _policy_key(event)
        assert key == ("policy.bound", "unknown", "unknown")


# ── AggregatePolicyWindow ──────────────────────────────────────────────────────

class TestAggregatePolicyWindow:
    def test_sums_premium_and_counts(self):
        events = [
            _policy_event(event_type="policy.bound"),
            {**_policy_event(event_type="policy.cancelled"),
             "payload": {**_policy_event()["payload"], "written_premium": 800.0}},
        ]
        key = ("policy.bound", "PersonalAutoLine", "CA")
        result = collect(AggregatePolicyWindow(), (key, events), window=FakeWindow())
        assert result[0]["event_count"] == 2
        assert result[0]["total_written_premium"] == pytest.approx(2000.0)
        assert result[0]["cancellation_count"] == 1

    def test_output_has_window_fields(self):
        key = ("policy.quoted", "HomeownersLine", "TX")
        result = collect(AggregatePolicyWindow(), (key, [_policy_event()]), window=FakeWindow())
        assert "window_start" in result[0]
        assert "window_end" in result[0]
        assert result[0]["line_of_business"] == "HomeownersLine"


# ── AggregateClaimWindow ───────────────────────────────────────────────────────

class TestAggregateClaimWindow:
    def test_sums_reserves_and_paid(self):
        e1 = _claim_event()
        e2 = {**_claim_event(), "payload": {**_claim_event()["payload"],
              "reserve_amount": 3000.0, "paid_to_date": 1500.0}}
        key = ("claim.reserved", "PersonalAutoLine")
        result = collect(AggregateClaimWindow(), (key, [e1, e2]), window=FakeWindow())
        assert result[0]["total_reserves"] == pytest.approx(8000.0)
        assert result[0]["total_paid"] == pytest.approx(1500.0)

    def test_cat_event_count(self):
        e_cat = {**_claim_event(),
                 "payload": {**_claim_event()["payload"], "catastrophe_code": "CAT-01"}}
        key = ("claim.fnol", "HomeownersLine")
        result = collect(AggregateClaimWindow(), (key, [_claim_event(), e_cat]), window=FakeWindow())
        assert result[0]["cat_event_count"] == 1


# ── AggregateBillingWindow ─────────────────────────────────────────────────────

class TestAggregateBillingWindow:
    def test_sums_collected_and_due(self):
        events = [_billing_event(), _billing_event()]
        result = collect(AggregateBillingWindow(), ("payment.received", events), window=FakeWindow())
        assert result[0]["total_amount_collected"] == pytest.approx(200.0)
        assert result[0]["total_amount_due"] == pytest.approx(200.0)
        assert result[0]["event_count"] == 2

    def test_counts_failures_and_delinquencies(self):
        failure = {**_billing_event(), "event_type": "payment.failed",
                   "payload": {**_billing_event()["payload"], "amount_paid": 0.0}}
        delq = {**_billing_event(), "event_type": "delinquency.opened"}
        result = collect(AggregateBillingWindow(),
                         ("payment.failed", [failure, delq]), window=FakeWindow())
        assert result[0]["payment_failure_count"] == 1
        assert result[0]["delinquency_count"] == 1


# ── Alert rules ────────────────────────────────────────────────────────────────

class TestAlertRules:
    def _agg(self, **kwargs):
        base = {
            "window_start": "2026-07-12T10:00:00",
            "window_end":   "2026-07-12T10:05:00",
            "event_type": "policy.bound", "line_of_business": "PersonalAutoLine",
            "state": "CA", "event_count": 20,
            "total_written_premium": 5000.0, "cancellation_count": 0,
            "computed_at": "2026-07-12T10:05:01",
        }
        base.update(kwargs)
        return base

    def test_no_alert_on_normal_cancellation_rate(self):
        agg = self._agg(event_count=20, cancellation_count=2)   # 10% — under threshold
        alerts = evaluate_policy_alerts(agg)
        assert not any(a["alert_type"] == "high_cancellation_rate" for a in alerts)

    def test_alert_on_high_cancellation_rate(self):
        agg = self._agg(event_count=20, cancellation_count=5)   # 25% — over threshold
        alerts = evaluate_policy_alerts(agg)
        assert any(a["alert_type"] == "high_cancellation_rate" for a in alerts)

    def test_cat_alert_fired(self):
        agg = {"window_start": "ts", "window_end": "ts", "event_type": "claim.fnol",
               "line_of_business": "HomeownersLine", "event_count": 10,
               "total_reserves": 50000.0, "total_paid": 0.0, "cat_event_count": 3,
               "computed_at": "ts"}
        alerts = evaluate_claim_alerts(agg)
        assert any(a["alert_type"] == "cat_event_spike" for a in alerts)

    def test_no_cat_alert_when_zero(self):
        agg = {"window_start": "ts", "window_end": "ts", "event_type": "claim.fnol",
               "line_of_business": "PersonalAutoLine", "event_count": 5,
               "total_reserves": 10000.0, "total_paid": 0.0, "cat_event_count": 0,
               "computed_at": "ts"}
        alerts = evaluate_claim_alerts(agg)
        assert not any(a["alert_type"] == "cat_event_spike" for a in alerts)

    def test_payment_failure_spike_alert(self):
        agg = {"window_start": "ts", "window_end": "ts", "event_type": "payment.failed",
               "event_count": 20, "total_amount_collected": 0.0, "total_amount_due": 2000.0,
               "delinquency_count": 0, "payment_failure_count": 6, "computed_at": "ts"}
        alerts = evaluate_billing_alerts(agg)
        assert any(a["alert_type"] == "payment_failure_spike" for a in alerts)
