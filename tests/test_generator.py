import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import jsonschema
import pytest

from ingestion.event_generator.generator import (
    build_billing_event,
    build_claim_event,
    build_policy_event,
)

SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "../ingestion/schemas")


def load_schema(name: str) -> dict:
    with open(os.path.join(SCHEMAS_DIR, name)) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def policy_schema():
    return load_schema("policy_event.json")


@pytest.fixture(scope="module")
def claim_schema():
    return load_schema("claim_event.json")


@pytest.fixture(scope="module")
def billing_schema():
    return load_schema("billing_event.json")


# ── Schema validation ─────���───────────────────────────────────────────────────

def test_policy_event_validates(policy_schema):
    for _ in range(50):
        jsonschema.validate(instance=build_policy_event(), schema=policy_schema)


def test_claim_event_validates(claim_schema):
    for _ in range(50):
        jsonschema.validate(instance=build_claim_event(), schema=claim_schema)


def test_billing_event_validates(billing_schema):
    for _ in range(50):
        jsonschema.validate(instance=build_billing_event(), schema=billing_schema)


# ── Domain invariants ───────────────────��──────────────────────────���──────────

def test_policy_event_guidewire_fields():
    e = build_policy_event()
    assert e["source_system"] == "guidewire_pc"
    assert e["domain"] == "policy"
    assert e["public_id"].startswith("pc:Policy:")
    assert e["payload"]["policy_number"].startswith("PC-")
    assert e["payload"]["currency"] == "USD"
    assert e["payload"]["written_premium"] > 0


def test_claim_event_guidewire_fields():
    e = build_claim_event()
    assert e["source_system"] == "guidewire_cc"
    assert e["domain"] == "claim"
    assert e["public_id"].startswith("cc:Claim:")
    assert e["payload"]["claim_number"].startswith("CL-")
    assert e["payload"]["reserve_amount"] >= 0
    assert isinstance(e["payload"]["subrogation_flag"], bool)
    assert isinstance(e["payload"]["litigation_flag"], bool)


def test_billing_event_guidewire_fields():
    e = build_billing_event()
    assert e["source_system"] == "guidewire_bc"
    assert e["domain"] == "billing"
    assert e["public_id"].startswith("bc:Account:")
    assert e["payload"]["currency"] == "USD"
    assert e["payload"]["outstanding_balance"] >= 0


# ── Conditional logic ─────────────────────────────────────────────────────────

def test_policy_cancellation_has_reason():
    cancelled = [build_policy_event() for _ in range(200)
                 if build_policy_event()["event_type"] == "policy.cancelled"]
    # generate specifically
    for _ in range(30):
        e = build_policy_event()
        if e["event_type"] == "policy.cancelled":
            assert e["payload"]["cancellation_reason"] is not None


def test_policy_endorsement_has_change_type():
    for _ in range(30):
        e = build_policy_event()
        if e["event_type"] == "policy.endorsed":
            assert e["payload"]["endorsement"] is not None
            assert e["payload"]["endorsement"]["change_type"] is not None


def test_claim_payment_has_payment_record():
    for _ in range(30):
        e = build_claim_event()
        if e["event_type"] == "claim.payment_issued":
            assert e["payload"]["payment"] is not None
            assert e["payload"]["payment"]["amount"] > 0


def test_billing_delinquency_has_days_past_due():
    for _ in range(30):
        e = build_billing_event()
        if e["event_type"] == "delinquency.opened":
            assert e["payload"]["days_past_due"] > 0
            assert e["payload"]["delinquency_reason"] is not None


def test_claim_fnol_has_no_adjuster():
    for _ in range(30):
        e = build_claim_event()
        if e["event_type"] == "claim.fnol":
            assert e["payload"]["assigned_adjuster"] is None
