"""
Guidewire Insurance mock event generator.

Simulates events from three Guidewire modules:
  - PolicyCenter  → Pub/Sub topic: pc-policy-events
  - ClaimCenter   → Pub/Sub topic: cc-claim-events
  - BillingCenter → Pub/Sub topic: bc-billing-events

Usage:
  python generator.py --project <gcp-project> --rate 10 --duration 60
  python generator.py --dry-run            # print to stdout, no Pub/Sub
  python generator.py --dry-run --domain policy  # single domain
"""

import argparse
import json
import random
import time
import uuid
from datetime import date, datetime, timedelta, timezone

try:
    from google.cloud import pubsub_v1
except ImportError:
    pubsub_v1 = None  # dry-run / tests work without GCP SDK

# ── Pub/Sub topics ─────────────────────────────────────────────────────────────

PC_TOPIC = "pc-policy-events"
CC_TOPIC = "cc-claim-events"
BC_TOPIC = "bc-billing-events"

SCHEMA_VERSION = "1.0"

# ── Reference data — mirrors Guidewire TypeKey lists ──────────────────────────

STATES = ["CA", "TX", "FL", "NY", "IL", "PA", "OH", "GA", "NC", "MI"]

POLICY_TYPES = [
    ("PersonalAuto",       "PersonalAutoLine",       0.40),
    ("Homeowners",         "HomeownersLine",          0.25),
    ("CommercialPackage",  "CommercialPropertyLine",  0.15),
    ("WorkersComp",        "WCLine",                  0.10),
    ("BusinessAuto",       "BusinessAutoLine",        0.07),
    ("UmbrellaPolicy",     "UmbrellaLine",            0.03),
]

PC_EVENT_TYPES = [
    ("policy.quoted",     "PolicyPeriod", "ui",    0.28),
    ("policy.bound",      "Policy",       "ui",    0.22),
    ("policy.endorsed",   "PolicyPeriod", "ui",    0.18),
    ("policy.renewed",    "PolicyPeriod", "batch", 0.15),
    ("policy.cancelled",  "Policy",       "api",   0.08),
    ("policy.lapsed",     "Policy",       "batch", 0.04),
    ("policy.reinstated", "Policy",       "api",   0.03),
    ("policy.expired",    "Policy",       "batch", 0.02),
]

UNDERWRITING_COMPANIES = [
    "NorCal Indemnity Co", "Southern Shield Insurance", "Heartland Mutual",
    "Atlantic Risk Partners", "Pacific Underwriters Ltd",
]

PRODUCERS = [f"AGT-{i:03d}" for i in range(1, 21)]

ENDORSEMENT_CHANGES = [
    "VehicleAdd", "VehicleRemove", "CoverageChange",
    "AddressChange", "DriverAdd", "DriverRemove",
]

CANCELLATION_REASONS = ["NonPayment", "UnderwritingIssue", "InsuredRequest", "Fraud"]

LOSS_TYPES_BY_LOB = {
    "PersonalAutoLine":      ["collision", "rollover", "theft", "vandalism", "glass", "bodily_injury"],
    "HomeownersLine":        ["fire", "weather", "theft", "vandalism", "slip_fall", "property_damage"],
    "CommercialPropertyLine":["fire", "weather", "vandalism", "property_damage", "slip_fall"],
    "WCLine":                ["slip_fall", "bodily_injury"],
    "BusinessAutoLine":      ["collision", "rollover", "property_damage", "bodily_injury"],
}

COVERAGE_BY_LOB = {
    "PersonalAutoLine":      ["CollisionCov", "ComprehensiveCov", "LiabilityCov", "MedPayCov", "UMCov", "UIMCov"],
    "HomeownersLine":        ["DwellingCov", "PersonalPropertyCov", "LiabilityCov"],
    "CommercialPropertyLine":["DwellingCov", "LiabilityCov", "PersonalPropertyCov"],
    "WCLine":                ["WorkersCompCov"],
    "BusinessAutoLine":      ["CollisionCov", "LiabilityCov", "ComprehensiveCov"],
}

ADJUSTER_TEAMS = ["Auto Physical Damage", "Auto Liability", "Property", "Workers Comp", "Commercial Lines"]

CC_EVENT_TYPES = [
    ("claim.fnol",               "Claim",        "fnol_portal", 0.25),
    ("claim.assigned",           "Claim",        "api",         0.18),
    ("claim.reserved",           "Exposure",     "api",         0.17),
    ("claim.coverage_verified",  "Exposure",     "api",         0.12),
    ("claim.payment_issued",     "ClaimPayment", "api",         0.10),
    ("claim.closed",             "Claim",        "ui",          0.08),
    ("claim.reopened",           "Claim",        "ui",          0.04),
    ("claim.escalated",          "Activity",     "ui",          0.03),
    ("claim.subrogation_opened", "Claim",        "api",         0.02),
    ("claim.litigation_flagged", "Claim",        "api",         0.01),
]

PAYMENT_TYPES = ["IndemnityPayment", "ExpensePayment", "SubrogationRecovery"]

BC_EVENT_TYPES = [
    ("invoice.generated",    "Invoice",        "batch",          0.25),
    ("invoice.due",          "Invoice",        "batch",          0.10),
    ("payment.received",     "Payment",        "payment_portal", 0.30),
    ("payment.failed",       "Payment",        "payment_portal", 0.10),
    ("payment.reversed",     "Payment",        "api",            0.04),
    ("delinquency.opened",   "Delinquency",    "batch",          0.07),
    ("delinquency.closed",   "Delinquency",    "batch",          0.05),
    ("refund.issued",        "Refund",         "api",            0.04),
    ("payment_plan.changed", "BillingAccount", "ui",             0.03),
    ("policy.written_off",   "BillingAccount", "batch",          0.02),
]

PAYMENT_METHODS   = ["ACH", "CreditCard", "Check", "EFT", "MoneyOrder"]
PAYMENT_PLANS     = ["Monthly", "Quarterly", "SemiAnnual", "Annual", "PayInFull"]
DELINQUENCY_WHY   = ["NSF", "CardDeclined", "CheckReturned", "MissedInstallment"]
DELINQUENCY_WF    = ["CancellationWorkflow", "ReinstatementWorkflow"]
FAILURE_REASONS   = ["Insufficient funds", "Card expired", "Account closed", "Routing number invalid"]

FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David", "Sarah"]
LAST_NAMES  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Wilson", "Moore"]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _pick_weighted(options):
    items   = [(o[0], o[1], o[2]) for o in options]
    weights = [o[-1] for o in options]
    return random.choices(options, weights=weights, k=1)[0]

def _future_date(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()

def _past_date(days: int) -> str:
    return (date.today() - timedelta(days=days)).isoformat()

def _pick_lob():
    return _pick_weighted(POLICY_TYPES)  # (policy_type, lob, weight)

def _policy_number() -> str:
    return f"PC-{random.randint(2024,2026)}-{random.randint(100000,999999)}"

def _claim_number() -> str:
    return f"CL-{random.randint(2024,2026)}-{random.randint(100000,999999)}"

def _billing_account_number() -> str:
    return f"BC-{random.randint(100000,999999)}"

def _invoice_number() -> str:
    return f"INV-{random.randint(10000,99999)}"

def _insured_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def _base_event(event_type, source_system, domain, entity_type, public_id, source, env):
    return {
        "event_id":      str(uuid.uuid4()),
        "event_type":    event_type,
        "source_system": source_system,
        "domain":        domain,
        "public_id":     public_id,
        "entity_type":   entity_type,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "source":        source,
        "metadata":      {"schema_version": SCHEMA_VERSION, "environment": env},
    }


# ── PolicyCenter event builder ─────────────────────────────────────────────────

def build_policy_event(env: str = "dev") -> dict:
    event_type, entity_type, source, _ = _pick_weighted(PC_EVENT_TYPES)
    policy_type, lob, _ = _pick_weighted(POLICY_TYPES)
    policy_id   = random.randint(10000, 99999)
    public_id   = f"pc:Policy:{policy_id}"
    policy_num  = _policy_number()
    state       = random.choice(STATES)
    premium     = round(random.uniform(400, 8000), 2)
    eff_date    = _past_date(random.randint(0, 180))
    exp_date    = _future_date(random.randint(1, 365))

    status_map = {
        "policy.quoted":     "Quoted",
        "policy.bound":      "Bound",
        "policy.endorsed":   "InForce",
        "policy.renewed":    "InForce",
        "policy.cancelled":  "Cancelled",
        "policy.lapsed":     "Lapsed",
        "policy.reinstated": "Reinstated",
        "policy.expired":    "Expired",
    }

    payload = {
        "policy_number":        policy_num,
        "policy_type":          policy_type,
        "account_number":       f"ACC-{random.randint(100000,999999)}",
        "line_of_business":     lob,
        "effective_date":       eff_date,
        "expiration_date":      exp_date,
        "term_type":            random.choice(["Annual", "SemiAnnual"]),
        "written_premium":      premium,
        "currency":             "USD",
        "state":                state,
        "underwriting_company": random.choice(UNDERWRITING_COMPANIES),
        "producer_code":        random.choice(PRODUCERS),
        "status":               status_map.get(event_type, "InForce"),
        "insured": {
            "contact_public_id": f"pc:Contact:{random.randint(1000,9999)}",
            "name":              _insured_name(),
            "dob":               _past_date(random.randint(7000, 25000)),
        },
        "cancellation_reason": random.choice(CANCELLATION_REASONS) if event_type == "policy.cancelled" else None,
        "endorsement": {
            "endorsement_number": f"E-{random.randint(1,20):03d}",
            "change_type":        random.choice(ENDORSEMENT_CHANGES),
            "premium_change":     round(random.uniform(-200, 400), 2),
        } if event_type == "policy.endorsed" else None,
    }

    event = _base_event(event_type, "guidewire_pc", "policy", entity_type, public_id, source, env)
    event["session_id"] = f"SES-{uuid.uuid4().hex[:8]}" if source == "ui" else None
    event["payload"]    = payload
    return event


# ── ClaimCenter event builder ──────────────────────────────────────────────────

def build_claim_event(env: str = "dev") -> dict:
    event_type, entity_type, source, _ = _pick_weighted(CC_EVENT_TYPES)
    _, lob, _ = _pick_weighted(POLICY_TYPES)
    claim_id   = random.randint(10000, 99999)
    public_id  = f"cc:Claim:{claim_id}"
    loss_date  = _past_date(random.randint(1, 90))
    rep_date   = _past_date(random.randint(0, 30))
    reserve    = round(random.uniform(500, 75000), 2)
    paid       = round(random.uniform(0, reserve * 0.8), 2) if event_type in (
        "claim.payment_issued", "claim.closed") else 0.0

    coverage_options = COVERAGE_BY_LOB.get(lob, ["LiabilityCov"])
    loss_options     = LOSS_TYPES_BY_LOB.get(lob, ["property_damage"])

    status_map = {
        "claim.fnol":               "New",
        "claim.assigned":           "Open",
        "claim.reserved":           "Open",
        "claim.coverage_verified":  "Open",
        "claim.payment_issued":     "Open",
        "claim.closed":             "Closed",
        "claim.reopened":           "Reopened",
        "claim.escalated":          "Open",
        "claim.subrogation_opened": "Open",
        "claim.litigation_flagged": "Open",
    }

    payload = {
        "claim_number":     _claim_number(),
        "policy_number":    _policy_number(),
        "line_of_business": lob,
        "loss_type":        random.choice(loss_options),
        "loss_date":        loss_date,
        "reported_date":    rep_date,
        "loss_location":    {"state": random.choice(STATES), "zip": f"{random.randint(10000,99999)}"},
        "coverage_type":    random.choice(coverage_options),
        "assigned_adjuster": f"ADJ-{random.randint(1,50):03d}" if event_type != "claim.fnol" else None,
        "adjuster_team":    random.choice(ADJUSTER_TEAMS) if event_type != "claim.fnol" else None,
        "reserve_amount":   reserve,
        "paid_to_date":     paid,
        "status":           status_map.get(event_type, "Open"),
        "catastrophe_code": f"CAT-{random.randint(1,20):02d}" if random.random() < 0.05 else None,
        "subrogation_flag": event_type == "claim.subrogation_opened",
        "litigation_flag":  event_type == "claim.litigation_flagged",
        "payment": {
            "payment_id":   f"pmt:{uuid.uuid4().hex[:8]}",
            "payment_type": random.choice(PAYMENT_TYPES),
            "amount":       paid,
            "payee":        _insured_name(),
        } if event_type == "claim.payment_issued" else None,
    }

    event = _base_event(event_type, "guidewire_cc", "claim", entity_type, public_id, source, env)
    event["payload"] = payload
    return event


# ── BillingCenter event builder ────────────────────────────────────────────────

def build_billing_event(env: str = "dev") -> dict:
    event_type, entity_type, source, _ = _pick_weighted(BC_EVENT_TYPES)
    acct_id    = random.randint(10000, 99999)
    public_id  = f"bc:Account:{acct_id}"
    amount_due = round(random.uniform(50, 800), 2)
    amount_paid = amount_due if event_type == "payment.received" else 0.0

    is_delinquent = event_type in ("delinquency.opened", "payment.failed", "payment.reversed", "policy.written_off")

    payload = {
        "billing_account_number": _billing_account_number(),
        "policy_number":          _policy_number(),
        "invoice_number":         _invoice_number() if event_type not in ("payment_plan.changed",) else None,
        "due_date":               _future_date(random.randint(0, 30)) if event_type in (
            "invoice.generated", "invoice.due") else None,
        "amount_due":             amount_due,
        "amount_paid":            amount_paid,
        "outstanding_balance":    round(amount_due - amount_paid, 2),
        "currency":               "USD",
        "payment_method":         random.choice(PAYMENT_METHODS) if event_type in (
            "payment.received", "payment.failed", "payment.reversed") else None,
        "payment_plan":           random.choice(PAYMENT_PLANS),
        "days_past_due":          random.randint(1, 60) if is_delinquent else 0,
        "delinquency_reason":     random.choice(DELINQUENCY_WHY) if event_type in (
            "delinquency.opened", "payment.failed") else None,
        "delinquency_workflow":   random.choice(DELINQUENCY_WF) if event_type == "delinquency.opened" else None,
        "failure_reason":         random.choice(FAILURE_REASONS) if event_type in (
            "payment.failed", "payment.reversed") else None,
    }

    event = _base_event(event_type, "guidewire_bc", "billing", entity_type, public_id, source, env)
    event["payload"] = payload
    return event


# ── Publisher ──────────────────────────────────────────────────────────────────

class EventPublisher:
    def __init__(self, project: str):
        if pubsub_v1 is None:
            raise RuntimeError("Install google-cloud-pubsub: pip install google-cloud-pubsub")
        self.client      = pubsub_v1.PublisherClient()
        self.project     = project
        self._topic_cache = {}

    def _topic_path(self, topic: str) -> str:
        if topic not in self._topic_cache:
            self._topic_cache[topic] = self.client.topic_path(self.project, topic)
        return self._topic_cache[topic]

    def publish(self, topic: str, event: dict) -> str:
        future = self.client.publish(
            self._topic_path(topic),
            data=json.dumps(event).encode("utf-8"),
            event_type=event["event_type"],
            domain=event["domain"],
            source_system=event["source_system"],
        )
        return future.result()


# ── Domain dispatch ────────────────────────────────────────────────────────────

# Weighted mix: PC 40%, CC 35%, BC 25% — mirrors typical Guidewire event volumes
DOMAIN_WEIGHTS = [
    ("policy",  build_policy_event,  PC_TOPIC,  0.40),
    ("claim",   build_claim_event,   CC_TOPIC,  0.35),
    ("billing", build_billing_event, BC_TOPIC,  0.25),
]


# ── Main loop ──────────────────────────────────────────────────────────────────

def run(project: str, rate: int, duration: int, env: str, dry_run: bool, domain: str | None):
    publisher = None if dry_run else EventPublisher(project)
    interval  = 1.0 / rate
    end_time  = time.time() + duration
    counts    = {"policy": 0, "claim": 0, "billing": 0}
    total     = 0

    domain_options = [d for d in DOMAIN_WEIGHTS if domain is None or d[0] == domain]
    if not domain_options:
        raise ValueError(f"Unknown domain: {domain}. Choose policy, claim, or billing.")

    print(f"[generator] Starting — rate={rate}/s  duration={duration}s  env={env}  dry_run={dry_run}")

    while time.time() < end_time:
        tick   = time.time()
        choice = _pick_weighted(domain_options)
        domain_name, builder, topic, _ = choice

        event = builder(env)

        if dry_run:
            print(f"\n[{topic}]\n{json.dumps(event, indent=2)}")
        else:
            msg_id = publisher.publish(topic, event)
            if total % 100 == 0:
                print(f"[generator] published={total}  {counts}  last_id={msg_id}")

        counts[domain_name] += 1
        total += 1

        sleep = interval - (time.time() - tick)
        if sleep > 0:
            time.sleep(sleep)

    print(f"[generator] Done — total={total}  {counts}")


def main():
    parser = argparse.ArgumentParser(description="Guidewire insurance mock event generator")
    parser.add_argument("--project",  default="your-gcp-project")
    parser.add_argument("--rate",     type=int, default=5,  help="Events per second")
    parser.add_argument("--duration", type=int, default=60, help="Run duration (seconds)")
    parser.add_argument("--env",      default="dev", choices=["dev", "staging", "prod"])
    parser.add_argument("--domain",   default=None, choices=["policy", "claim", "billing"],
                        help="Emit only this domain (default: all)")
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    run(project=args.project, rate=args.rate, duration=args.duration,
        env=args.env, dry_run=args.dry_run, domain=args.domain)


if __name__ == "__main__":
    main()
