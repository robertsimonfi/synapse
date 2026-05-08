"""
Synapse Operator Portal — Streamlit HITL dashboard.

Operators use this UI to:
  • See a live queue of PENDING AI-proposed actions
  • Inspect the before/after state diff for each action
  • Approve (one click) or Reject (with mandatory reason) actions
  • Monitor queue health via sidebar metrics

Run:
  streamlit run ui/operator_portal.py
Or via docker-compose — see the 'ui' service.

Config env vars:
  API_BASE_URL     FastAPI base URL (default: http://localhost:8000)
  SYNAPSE_API_KEY  X-API-Key header value
"""

import os

import pandas as pd
import requests
import streamlit as st

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
API_KEY  = os.getenv("SYNAPSE_API_KEY", "")
HEADERS  = {"X-API-Key": API_KEY}

st.set_page_config(
    page_title="Synapse Operator Portal",
    page_icon="🔁",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── API helpers ────────────────────────────────────────────────────────────────

def _get(path: str, **params) -> list | dict | None:
    try:
        r = requests.get(f"{API_BASE}{path}", headers=HEADERS, params=params, timeout=8)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        st.error(f"Cannot reach API at {API_BASE} — is the backend running?")
        return None
    except Exception as exc:
        st.error(f"API error: {exc}")
        return None


def _post(path: str, payload: dict) -> dict | None:
    try:
        r = requests.post(f"{API_BASE}{path}", headers=HEADERS, json=payload, timeout=8)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as exc:
        detail = exc.response.json().get("detail", str(exc)) if exc.response else str(exc)
        st.error(f"Error: {detail}")
        return None
    except Exception as exc:
        st.error(f"Request failed: {exc}")
        return None


# ── Sidebar ────────────────────────────────────────────────────────────────────

def render_sidebar() -> str:
    st.sidebar.title("🔁 Synapse")
    st.sidebar.markdown("**Operator Portal**")
    st.sidebar.divider()

    depths = _get("/actions/depths") or {}
    pending          = depths.get("PENDING",          0)
    approved         = depths.get("APPROVED",         0)
    exec_            = depths.get("EXECUTED",         0)
    failed           = depths.get("FAILED",           0)
    expired          = depths.get("EXPIRED",          0)
    rejected         = depths.get("REJECTED",         0)
    stale            = depths.get("STALE",            0)
    escalated_stale  = depths.get("ESCALATED_STALE",  0)

    col1, col2 = st.sidebar.columns(2)
    col1.metric("Pending",  pending,  delta=None)
    col2.metric("Approved", approved, delta=None)
    col1.metric("Executed", exec_)
    col2.metric("Failed",   failed,   delta=failed  if failed  else None, delta_color="inverse")
    col1.metric("Expired",  expired,  delta=expired if expired else None, delta_color="inverse")
    col2.metric("Rejected", rejected)

    # Phase 8 — Defensive Execution metrics
    if stale or escalated_stale:
        st.sidebar.divider()
        col3, col4 = st.sidebar.columns(2)
        col3.metric("Stale",     stale,           delta=stale           if stale           else None, delta_color="inverse")
        col4.metric("Escalated", escalated_stale, delta=escalated_stale if escalated_stale else None, delta_color="inverse")

    st.sidebar.divider()
    operator_id = st.sidebar.text_input(
        "Your Operator ID",
        placeholder="operator-alice-42",
        help="Required before approving or rejecting actions.",
    )

    if escalated_stale:
        st.sidebar.error(f"🚨 {escalated_stale} action(s) ESCALATED — human review required!")
    elif pending > 5:
        st.sidebar.warning(f"⚠ {pending} actions awaiting review!")
    elif pending == 0 and escalated_stale == 0:
        st.sidebar.success("✅ Queue clear — no pending actions.")

    return operator_id


# ── Action list view ───────────────────────────────────────────────────────────

def render_main(operator_id: str) -> None:
    st.title("Action Review Queue")

    tab_pending, tab_approved, tab_history, tab_failed, tab_stale, tab_engagement = st.tabs([
        "⏳ Pending Review",
        "✅ Approved",
        "📋 History",
        "❌ Failed / Expired",
        "⚠️ Stale / Escalated",
        "📣 Customer Engagement",
    ])

    with tab_pending:
        actions = _get("/actions", status="PENDING", limit=100) or []
        if not actions:
            st.info("No pending actions — all clear!")
        else:
            st.caption(f"{len(actions)} action(s) awaiting your decision.")
            for action in actions:
                _render_action_card(action, operator_id, show_controls=True)

    with tab_approved:
        actions = _get("/actions", status="APPROVED", limit=50) or []
        if not actions:
            st.info("No approved actions waiting to be executed.")
        for action in actions:
            with st.expander(
                f"[{action['action_type'].upper()}] {action['action_description'][:90]}",
                expanded=False,
            ):
                _render_meta(action)
                st.caption(f"Approved by: **{action.get('approved_by', '—')}**")

    with tab_history:
        col_exec, col_rej = st.columns(2)
        with col_exec:
            st.subheader("Executed")
            actions = _get("/actions", status="EXECUTED", limit=50) or []
            for a in actions:
                st.markdown(
                    f"✅ `{a['id'][:8]}` **{a['action_type']}** — "
                    f"{a['action_description'][:60]}"
                )
        with col_rej:
            st.subheader("Rejected")
            actions = _get("/actions", status="REJECTED", limit=50) or []
            for a in actions:
                st.markdown(
                    f"❌ `{a['id'][:8]}` **{a['action_type']}** — "
                    f"_{a.get('rejection_reason', 'no reason')[:60]}_"
                )

    with tab_failed:
        col_fail, col_exp = st.columns(2)
        with col_fail:
            st.subheader("Failed (DLQ)")
            actions = _get("/actions", status="FAILED", limit=50) or []
            for a in actions:
                st.markdown(
                    f"💥 `{a['id'][:8]}` **{a['action_type']}** — "
                    f"_{a.get('execution_error', 'unknown error')[:60]}_"
                )
        with col_exp:
            st.subheader("Expired")
            actions = _get("/actions", status="EXPIRED", limit=50) or []
            for a in actions:
                st.markdown(
                    f"⌛ `{a['id'][:8]}` **{a['action_type']}** — "
                    f"created {a['created_at'][:10]}"
                )

    with tab_stale:
        _render_stale_tab()

    with tab_engagement:
        _render_engagement_tab()


# ── Stale / Escalated tab (Phase 8) ──────────────────────────────────────────

def _render_stale_tab() -> None:
    st.subheader("Stale & Escalated Actions")
    st.caption(
        "Actions the Watchdog detected as stale (CRM state changed since analysis). "
        "ESCALATED actions require manual operator decision — the circuit breaker fired."
    )

    col_stale, col_esc = st.columns(2)

    with col_stale:
        st.markdown("#### 🟡 Stale — Re-analysing")
        st.caption("Marked STALE by Watchdog; a fresh analysis has been re-queued.")
        actions = _get("/actions", status="STALE", limit=50) or []
        if not actions:
            st.info("No stale actions.")
        for a in actions:
            reanalysis = a.get("reanalysis_count", 0)
            reason     = a.get("stale_reason", "unknown reason")
            with st.expander(
                f"🟡 `{a['id'][:8]}` **{a['action_type']}** "
                f"— attempt {reanalysis}/2",
                expanded=False,
            ):
                st.markdown(f"**Action ID:** `{a['id']}`")
                st.markdown(f"**Type:** `{a['action_type']}`")
                st.markdown(f"**Customer:** `{a.get('customer_id', '—')}`")
                st.markdown(f"**Order:** `{a.get('order_id', '—')}`")
                st.markdown(f"**Re-analysis attempt:** {reanalysis} / 2")
                st.warning(f"**Drift reason:** {reason}")
                st.caption(f"Created: {a.get('created_at', '—')[:19]}")

    with col_esc:
        st.markdown("#### 🔴 Escalated — Human Review Required")
        st.caption(
            "Watchdog circuit breaker fired. The account state is too volatile "
            "for autonomous re-analysis. Operator must review and act manually."
        )
        actions = _get("/actions", status="ESCALATED_STALE", limit=50) or []
        if not actions:
            st.info("No escalated actions.")
        for a in actions:
            reason = a.get("stale_reason", "unknown reason")
            with st.expander(
                f"🔴 `{a['id'][:8]}` **{a['action_type']}** — ESCALATED",
                expanded=True,
            ):
                st.markdown(f"**Action ID:** `{a['id']}`")
                st.markdown(f"**Type:** `{a['action_type']}`")
                st.markdown(f"**Customer:** `{a.get('customer_id', '—')}`")
                st.markdown(f"**Order:** `{a.get('order_id', '—')}`")
                st.error(f"**Escalation reason:** {reason}")
                st.markdown("**Original proposal:**")
                st.json(a.get("action_payload", {}))
                st.caption(f"Created: {a.get('created_at', '—')[:19]}")


# ── Customer Engagement tab ───────────────────────────────────────────────────

_RESPONSE_ICON = {
    "PENDING":  "⏳",
    "ACCEPTED": "✅",
    "DECLINED": "❌",
    "REPLIED":  "💬",
}

_STATUS_ICON = {
    "SENT":    "📤",
    "PENDING": "🕐",
    "FAILED":  "💥",
}

_CHANNEL_ICON = {
    "SMS":      "📱",
    "EMAIL":    "✉️",
    "WHATSAPP": "💚",
}


def _render_engagement_tab() -> None:
    st.subheader("Customer Engagement Log")
    st.caption("Proactive notifications sent after action execution, and customer replies.")

    col_filter, col_refresh = st.columns([4, 1])
    with col_filter:
        filter_action = st.text_input(
            "Filter by Action ID (optional)", placeholder="paste action UUID prefix…"
        )
    with col_refresh:
        st.write("")
        refresh = st.button("🔄 Refresh")

    logs = _get("/engagements", limit=100, **({"action_id": filter_action} if filter_action else {})) or []

    if not logs:
        st.info("No engagement records yet. They appear here after actions are executed.")
        return

    # Summary metrics row
    total   = len(logs)
    sent    = sum(1 for e in logs if e["status"] == "SENT")
    replied = sum(1 for e in logs if e["customer_response"] == "REPLIED")
    failed  = sum(1 for e in logs if e["status"] == "FAILED")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Sent", sent)
    m2.metric("Replies Received", replied)
    m3.metric("Delivery Failures", failed, delta=failed if failed else None, delta_color="inverse")
    m4.metric("Total Engagements", total)

    st.divider()

    # Engagement cards
    for eng in logs:
        chan_icon = _CHANNEL_ICON.get(eng["channel"], "📨")
        resp_icon = _RESPONSE_ICON.get(eng["customer_response"], "⏳")
        stat_icon = _STATUS_ICON.get(eng["status"], "")
        label     = (
            f"{chan_icon} [{eng['channel']}] {stat_icon} {eng['status']} "
            f"→ {resp_icon} {eng['customer_response']} "
            f"— action `{eng['action_id'][:8]}` "
            f"— {eng['created_at'][:10]}"
        )
        with st.expander(label, expanded=False):
            col_info, col_msg = st.columns([1, 2])

            with col_info:
                st.markdown(f"**Engagement ID:** `{eng['id']}`")
                st.markdown(f"**Action ID:** `{eng['action_id']}`")
                st.markdown(f"**Correlation:** `{eng['correlation_id']}`")
                st.markdown(f"**Customer ID:** `{eng.get('customer_id', '—')}`")
                st.markdown(f"**Channel:** {chan_icon} `{eng['channel']}`")
                st.markdown(f"**Template:** `{eng['template_used']}`")
                st.markdown(f"**Recipient (masked):** `{eng['recipient_masked']}`")
                st.markdown(f"**Sent at:** {eng.get('sent_at', '—')}")

            with col_msg:
                st.markdown("**Message sent to customer:**")
                st.info(eng["message_body"])

                if eng["customer_response"] != "PENDING":
                    st.markdown(
                        f"**Customer replied** ({resp_icon} `{eng['customer_response']}`) "
                        f"at {eng.get('response_received_at', '—')[:19]}:"
                    )
                    st.success(eng.get("response_text", "_(no text)_"))
                else:
                    st.caption("Awaiting customer response…")


# ── Action card ────────────────────────────────────────────────────────────────

def _render_action_card(action: dict, operator_id: str, show_controls: bool) -> None:
    action_id   = action["id"]
    action_type = action["action_type"].upper()
    description = action["action_description"]

    with st.expander(f"[{action_type}] {description[:90]}", expanded=False):
        col_meta, col_diff = st.columns([1, 2])

        with col_meta:
            _render_meta(action)

        with col_diff:
            st.subheader("State Diff")
            before = action.get("predicted_before_state", {})
            after  = action.get("predicted_after_state",  {})

            if before or after:
                all_keys = sorted(set(before) | set(after))
                df = pd.DataFrame([
                    {"Field": k, "Before": before.get(k, "—"), "After": after.get(k, "—")}
                    for k in all_keys
                ])
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.caption("No state diff available for this action.")

            if action.get("action_payload"):
                with st.expander("Execution payload", expanded=False):
                    st.json(action["action_payload"])

        if show_controls:
            st.divider()
            col_approve, col_sep, col_reject = st.columns([2, 0.2, 3])

            with col_approve:
                if st.button("✅ Approve", key=f"approve_{action_id}", type="primary"):
                    if not operator_id:
                        st.error("Set your Operator ID in the sidebar first.")
                    else:
                        result = _post(
                            f"/actions/{action_id}/approve",
                            {"operator_id": operator_id},
                        )
                        if result:
                            st.success(f"Approved — executor will pick this up shortly.")
                            st.rerun()

            with col_reject:
                reason = st.text_input(
                    "Rejection reason (required)",
                    key=f"reason_{action_id}",
                    placeholder="Why is this action not appropriate right now?",
                )
                if st.button("❌ Reject", key=f"reject_{action_id}"):
                    if not operator_id:
                        st.error("Set your Operator ID in the sidebar first.")
                    elif not reason or len(reason.strip()) < 5:
                        st.error("Provide a meaningful rejection reason (min 5 chars).")
                    else:
                        result = _post(
                            f"/actions/{action_id}/reject",
                            {"operator_id": operator_id, "reason": reason},
                        )
                        if result:
                            st.warning("Action rejected and recorded.")
                            st.rerun()


def _render_meta(action: dict) -> None:
    st.markdown(f"**ID:** `{action['id']}`")
    st.markdown(f"**Correlation:** `{action.get('correlation_id', '—')}`")
    st.markdown(f"**Order ID:** `{action.get('order_id', '—')}`")
    st.markdown(f"**Customer ID:** `{action.get('customer_id', '—')}`")
    st.markdown(f"**Status:** `{action['status']}`")
    st.markdown(f"**Created:** {action.get('created_at', '—')[:19]}")
    st.markdown(f"**Expires:** {action.get('expires_at', '—')[:19]}")

    if action.get("approved_by"):
        st.markdown(f"**Approved by:** {action['approved_by']}")
    if action.get("rejection_reason"):
        st.markdown(f"**Rejected:** _{action['rejection_reason']}_")
    if action.get("execution_error"):
        st.markdown(f"**Error:** _{action['execution_error']}_")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    operator_id = render_sidebar()
    render_main(operator_id)


if __name__ == "__main__":
    main()
