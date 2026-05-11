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

    tab_pending, tab_approved, tab_history, tab_failed, tab_stale, tab_dlq, tab_engagement, tab_audit = st.tabs([
        "⏳ Pending Review",
        "✅ Approved",
        "📋 History",
        "❌ Failed / Expired",
        "⚠️ Stale / Escalated",
        "☠️ DLQ",
        "📣 Customer Engagement",
        "📒 Audit Log",
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
            if not actions:
                st.info("No failed actions.")
            for a in actions:
                err   = a.get("execution_error") or "unknown error"
                tries = a.get("handler_attempt_count") or a.get("retry_count", "?")
                label = f"💥 `{a['id'][:8]}` **{a['action_type']}** — {err[:60]}"
                with st.expander(label, expanded=False):
                    st.error(f"**Error:** {err}")
                    st.markdown(f"**Retry attempts:** {tries}")
                    comp_type   = a.get("compensation_type")
                    comp_result = a.get("compensation_result")
                    if comp_type:
                        st.info(f"**Compensation:** `{comp_type}`")
                        if comp_result:
                            st.write("**Compensation result:**")
                            st.json(comp_result)
                    st.caption(f"Customer: {a.get('customer_id', '—')} | Order: {a.get('order_id', '—')}")
        with col_exp:
            st.subheader("Expired")
            actions = _get("/actions", status="EXPIRED", limit=50) or []
            for a in actions:
                st.markdown(
                    f"⌛ `{a['id'][:8]}` **{a['action_type']}** — "
                    f"created {a['created_at'][:10]}"
                )

    with tab_stale:
        _render_stale_tab(operator_id)

    with tab_dlq:
        _render_dlq_tab(operator_id)

    with tab_engagement:
        _render_engagement_tab()

    with tab_audit:
        _render_audit_log_tab()


# ── Stale / Escalated tab (Phase 8) ──────────────────────────────────────────

def _render_stale_tab(operator_id: str) -> None:
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

                predicted = a.get("predicted_state_at_check")
                current   = a.get("current_state_at_check")
                if predicted or current:
                    sc1, sc2 = st.columns(2)
                    with sc1:
                        st.write("**Predicted state (at check):**")
                        st.code(str(predicted or "—"))
                    with sc2:
                        st.write("**Current state (at check):**")
                        st.code(str(current or "—"))

                st.caption(f"Created: {a.get('created_at', '—')[:19]}")

    with col_esc:
        st.markdown("#### 🔴 Escalated — Human Review Required")
        st.caption(
            "Watchdog circuit breaker fired. The account state is too volatile "
            "for autonomous re-analysis. Operator may force-execute if the drift "
            "is a known artefact (e.g. caching, in-flight payment clearing)."
        )
        actions = _get("/actions", status="ESCALATED_STALE", limit=50) or []
        if not actions:
            st.info("No escalated actions.")
        for a in actions:
            action_id = a["id"]
            reason    = a.get("stale_reason", "unknown reason")
            with st.expander(
                f"🔴 `{action_id[:8]}` **{a['action_type']}** — ESCALATED",
                expanded=True,
            ):
                st.markdown(f"**Action ID:** `{action_id}`")
                st.markdown(f"**Type:** `{a['action_type']}`")
                st.markdown(f"**Customer:** `{a.get('customer_id', '—')}`")
                st.markdown(f"**Order:** `{a.get('order_id', '—')}`")
                st.error(f"**Escalation reason:** {reason}")
                st.markdown("**Original proposal:**")
                st.json(a.get("action_payload", {}))
                st.caption(f"Created: {a.get('created_at', '—')[:19]}")

                # ── Phase 10: Force Execute override ─────────────────────────
                st.divider()
                st.warning(
                    "⚠️ **Force Execute** bypasses drift validation. "
                    "Use only when you have confirmed the stale state is a known "
                    "system artefact (e.g. cache lag, payment clearing delay)."
                )
                justification = st.text_area(
                    "Justification (required — min 10 chars)",
                    key=f"justification_{action_id}",
                    placeholder="e.g. Confirmed with Network Ops — state drift is CDN cache lag, safe to proceed.",
                    height=80,
                )
                if st.button(
                    "🚨 Force Execute (bypass drift check)",
                    key=f"force_{action_id}",
                    type="primary",
                ):
                    if not operator_id:
                        st.error("Set your Operator ID in the sidebar first.")
                    elif not justification or len(justification.strip()) < 10:
                        st.error("Provide a meaningful justification (min 10 chars).")
                    else:
                        result = _post(
                            f"/actions/{action_id}/force-approve",
                            {"operator_id": operator_id, "justification": justification},
                        )
                        if result:
                            st.success(
                                f"Action {action_id[:8]} marked FORCE_APPROVED — "
                                "Executor will pick it up on the next poll cycle."
                            )
                            st.rerun()


# ── DLQ tab (Phase 10) ────────────────────────────────────────────────────────

def _render_dlq_tab(operator_id: str) -> None:
    st.subheader("Dead Letter Queue — Failed Oracle Write-backs")
    st.caption(
        "Commands that exhausted all retry attempts in the Oracle Adapter. "
        "Each entry represents a failed Oracle DML that needs manual repair or deliberate discard."
    )

    col_filter, col_refresh = st.columns([3, 1])
    with col_filter:
        status_filter = st.selectbox(
            "Filter by status", ["OPEN", "ACKNOWLEDGED", "ALL"], index=0
        )
    with col_refresh:
        st.write("")
        st.button("🔄 Refresh", key="dlq_refresh")

    events = _get("/dlq", status=status_filter, limit=200) or []

    if not events:
        st.info("No DLQ events — write-back pipeline is healthy.")
        return

    open_count = sum(1 for e in events if e.get("status") == "OPEN")
    ack_count  = sum(1 for e in events if e.get("status") == "ACKNOWLEDGED")

    m1, m2, m3 = st.columns(3)
    m1.metric("Open", open_count,  delta=open_count if open_count else None, delta_color="inverse")
    m2.metric("Acknowledged", ack_count)
    m3.metric("Total shown", len(events))

    st.divider()

    for evt in events:
        outbox_id   = evt.get("outbox_id", evt.get("dlq_id", "?"))
        action_type = evt.get("action_type", "unknown")
        table       = evt.get("table_name", "?")
        attempts    = evt.get("attempts", "?")
        status      = evt.get("status", "OPEN")
        icon        = "🔴" if status == "OPEN" else "✅"

        label = (
            f"{icon} `{outbox_id[:8]}` **{action_type}** → `{table}` "
            f"— {attempts} attempt(s) — {evt.get('failed_at', '?')[:19]}"
        )
        with st.expander(label, expanded=(status == "OPEN")):
            col_meta, col_err = st.columns([1, 2])

            with col_meta:
                st.markdown(f"**Outbox ID:** `{outbox_id}`")
                st.markdown(f"**Action ID:** `{evt.get('action_id', '—')}`")
                st.markdown(f"**Correlation:** `{evt.get('correlation_id', '—')}`")
                st.markdown(f"**Action type:** `{action_type}`")
                st.markdown(f"**Table:** `{table}`")
                st.markdown(f"**Attempts:** {attempts}")
                st.markdown(f"**Status:** `{status}`")
                trace_id = evt.get("trace_id")
                if trace_id:
                    st.markdown(f"**Trace ID:** `{trace_id}`")
                st.caption(f"Failed at: {evt.get('failed_at', '—')[:19]}")

            with col_err:
                st.markdown("**Error context:**")
                st.error(evt.get("error_context", "No error detail available."))
                if evt.get("where_clause"):
                    st.markdown("**WHERE clause:**")
                    st.json(evt["where_clause"])
                if evt.get("set_values"):
                    st.markdown("**SET values:**")
                    st.json(evt["set_values"])

            if status == "OPEN":
                st.divider()
                if st.button(
                    "✅ Acknowledge (mark as reviewed)",
                    key=f"dlq_ack_{outbox_id}",
                ):
                    if not operator_id:
                        st.error("Set your Operator ID in the sidebar first.")
                    else:
                        result = _post(
                            f"/dlq/{outbox_id}/acknowledge",
                            {"operator_id": operator_id},
                        )
                        if result:
                            st.success("Event acknowledged.")
                            st.rerun()


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


# ── Audit Log tab ─────────────────────────────────────────────────────────────

def _render_audit_log_tab() -> None:
    st.subheader("Audit Log — Force Approved & Failed Actions")
    st.caption(
        "All FORCE_APPROVED and FAILED actions. Use the filters to narrow the view, "
        "then export to CSV for compliance or post-incident review."
    )

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        date_from = st.date_input("From date", key="audit_from")
    with col2:
        date_to   = st.date_input("To date",   key="audit_to")
    with col3:
        st.write("")
        refresh = st.button("🔄 Refresh", key="audit_refresh")

    col4, col5 = st.columns(2)
    with col4:
        status_filter = st.selectbox(
            "Status", ["ALL", "FORCE_APPROVED", "FAILED"], index=0, key="audit_status"
        )
    with col5:
        action_type_filter = st.text_input(
            "Action type (optional)", placeholder="e.g. retry_provisioning", key="audit_type"
        )

    # Collect actions for chosen statuses
    statuses = (
        ["FORCE_APPROVED", "FAILED"]
        if status_filter == "ALL"
        else [status_filter]
    )
    rows = []
    for s in statuses:
        actions = _get("/actions", status=s, limit=500) or []
        for a in actions:
            if action_type_filter and action_type_filter not in a.get("action_type", ""):
                continue
            created = a.get("created_at", "")[:10]
            if str(date_from) > created or created > str(date_to):
                continue
            rows.append({
                "Timestamp":    a.get("updated_at", a.get("created_at", ""))[:19],
                "Operator":     a.get("approved_by", "—"),
                "Action type":  a.get("action_type", "—"),
                "Order ID":     a.get("order_id", "—"),
                "Customer ID":  a.get("customer_id", "—"),
                "Status":       a.get("status", "—"),
                "Approval method": a.get("approval_method") or "—",
                "Reason / Error": (
                    a.get("force_approval_reason")
                    or (a.get("execution_error") or "")[:120]
                    or "—"
                ),
            })

    if not rows:
        st.info("No matching audit records for the selected filters.")
        return

    df = pd.DataFrame(rows).sort_values("Timestamp", ascending=False)
    st.dataframe(df, use_container_width=True, hide_index=True)

    csv = df.to_csv(index=False)
    st.download_button(
        label="⬇️ Download as CSV",
        data=csv,
        file_name="synapse_audit_log.csv",
        mime="text/csv",
    )


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
            col_approve, col_force, col_reject = st.columns([2, 2, 3])

            with col_approve:
                if st.button("✅ Approve", key=f"approve_{action_id}", type="primary"):
                    if not operator_id:
                        st.error("Set your Operator ID in the sidebar first.")
                    else:
                        result = _post(
                            f"/actions/{action_id}/approve",
                            {"operator_id": operator_id, "approval_method": "STANDARD"},
                        )
                        if result:
                            st.success("Approved — executor will pick this up shortly.")
                            st.rerun()

            with col_force:
                if st.button("⚡ Force Approve", key=f"force_btn_{action_id}"):
                    st.session_state[f"show_force_modal_{action_id}"] = True

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

            if st.session_state.get(f"show_force_modal_{action_id}"):
                _render_force_approval_modal(action, operator_id)


def _render_force_approval_modal(action: dict, operator_id: str) -> None:
    action_id = action["id"]
    st.divider()
    st.markdown("### ⚠️ SAFETY CHECK BYPASS")
    st.warning(
        "You are about to execute this action **WITHOUT** pre-flight drift detection. "
        "The executor will skip the CRM state check and execute immediately."
    )

    col1, col2 = st.columns(2)
    with col1:
        st.write("**Predicted before state:**")
        st.code(str(action.get("predicted_before_state", {})))
    with col2:
        st.write("**Predicted after state:**")
        st.code(str(action.get("predicted_after_state", {})))

    force_reason = st.text_area(
        "Why are you using Force Approve? (required)",
        key=f"force_reason_{action_id}",
        placeholder="E.g., Critical outage — customer out of service for 30 mins. Confirmed with NOC.",
        height=80,
    )
    confirm = st.checkbox(
        "I understand the risks and take full responsibility for bypassing safety checks",
        key=f"force_confirm_{action_id}",
        value=False,
    )

    col_cancel, col_submit = st.columns(2)
    with col_cancel:
        if st.button("Cancel", key=f"force_cancel_{action_id}"):
            st.session_state[f"show_force_modal_{action_id}"] = False
            st.rerun()
    with col_submit:
        disabled = not (confirm and force_reason and len(force_reason.strip()) >= 10)
        if st.button(
            "⚡ Force Approve — I understand the risks",
            key=f"force_submit_{action_id}",
            type="primary",
            disabled=disabled,
        ):
            if not operator_id:
                st.error("Set your Operator ID in the sidebar first.")
            else:
                result = _post(
                    f"/actions/{action_id}/approve",
                    {
                        "operator_id": operator_id,
                        "approval_method": "FORCE_APPROVED",
                        "force_approval_reason": force_reason.strip(),
                    },
                )
                if result:
                    st.success(f"Action {action_id[:8]} force approved — executor will pick it up shortly.")
                    st.session_state[f"show_force_modal_{action_id}"] = False
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
