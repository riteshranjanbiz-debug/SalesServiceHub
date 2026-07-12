# ═══════════════════════════════════════════════════════════════════════════════
# PolicyCenter — pc-policy-events
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_pubsub_topic" "pc_policy_events" {
  name                       = "pc-policy-events"
  project                    = var.project_id
  message_retention_duration = "86400s"
  labels                     = { env = var.env, domain = "policy", source_system = "guidewire_pc" }
}

resource "google_pubsub_topic" "pc_policy_events_dlq" {
  name    = "pc-policy-events-dlq"
  project = var.project_id
  labels  = { env = var.env, domain = "policy", purpose = "dlq" }
}

resource "google_pubsub_subscription" "pc_policy_events_dataflow" {
  name    = "pc-policy-events-dataflow-sub"
  topic   = google_pubsub_topic.pc_policy_events.name
  project = var.project_id

  ack_deadline_seconds       = 60
  retain_acked_messages      = false
  message_retention_duration = "86400s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pc_policy_events_dlq.id
    max_delivery_attempts = 5
  }

  labels = { env = var.env, domain = "policy" }
}

# ═══════════════════════════════════════════════════════════════════════════════
# ClaimCenter — cc-claim-events
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_pubsub_topic" "cc_claim_events" {
  name                       = "cc-claim-events"
  project                    = var.project_id
  message_retention_duration = "86400s"
  labels                     = { env = var.env, domain = "claim", source_system = "guidewire_cc" }
}

resource "google_pubsub_topic" "cc_claim_events_dlq" {
  name    = "cc-claim-events-dlq"
  project = var.project_id
  labels  = { env = var.env, domain = "claim", purpose = "dlq" }
}

resource "google_pubsub_subscription" "cc_claim_events_dataflow" {
  name    = "cc-claim-events-dataflow-sub"
  topic   = google_pubsub_topic.cc_claim_events.name
  project = var.project_id

  ack_deadline_seconds       = 60
  retain_acked_messages      = false
  message_retention_duration = "86400s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.cc_claim_events_dlq.id
    max_delivery_attempts = 5
  }

  labels = { env = var.env, domain = "claim" }
}

# ═══════════════════════════════════════════════════════════════════════════════
# BillingCenter — bc-billing-events
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_pubsub_topic" "bc_billing_events" {
  name                       = "bc-billing-events"
  project                    = var.project_id
  message_retention_duration = "86400s"
  labels                     = { env = var.env, domain = "billing", source_system = "guidewire_bc" }
}

resource "google_pubsub_topic" "bc_billing_events_dlq" {
  name    = "bc-billing-events-dlq"
  project = var.project_id
  labels  = { env = var.env, domain = "billing", purpose = "dlq" }
}

resource "google_pubsub_subscription" "bc_billing_events_dataflow" {
  name    = "bc-billing-events-dataflow-sub"
  topic   = google_pubsub_topic.bc_billing_events.name
  project = var.project_id

  ack_deadline_seconds       = 60
  retain_acked_messages      = false
  message_retention_duration = "86400s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.bc_billing_events_dlq.id
    max_delivery_attempts = 5
  }

  labels = { env = var.env, domain = "billing" }
}

# ═══════════════════════════════════════════════════════════════════════════════
# Shared DLQ subscription (for monitoring/alerting)
# ═══════════════════════════════════════════════════════════════════════════════

resource "google_pubsub_subscription" "pc_dlq_monitor" {
  name                 = "pc-policy-events-dlq-monitor-sub"
  topic                = google_pubsub_topic.pc_policy_events_dlq.name
  project              = var.project_id
  ack_deadline_seconds = 60
  labels               = { env = var.env, purpose = "dlq-monitor" }
}

resource "google_pubsub_subscription" "cc_dlq_monitor" {
  name                 = "cc-claim-events-dlq-monitor-sub"
  topic                = google_pubsub_topic.cc_claim_events_dlq.name
  project              = var.project_id
  ack_deadline_seconds = 60
  labels               = { env = var.env, purpose = "dlq-monitor" }
}

resource "google_pubsub_subscription" "bc_dlq_monitor" {
  name                 = "bc-billing-events-dlq-monitor-sub"
  topic                = google_pubsub_topic.bc_billing_events_dlq.name
  project              = var.project_id
  ack_deadline_seconds = 60
  labels               = { env = var.env, purpose = "dlq-monitor" }
}
