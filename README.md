# Telco Fallout Resolution Agent

An AI-powered agent that detects, analyzes, and resolves operational errors across BSS and OSS systems for Telco environments (T-Mobile style). Built with the Anthropic SDK, ChromaDB RAG, Oracle DB, and FastAPI.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   FastAPI REST API                  │
│              POST /resolve   GET /health            │
└────────────────────┬────────────────────────────────┘
                     │
              ┌──────▼──────┐
              │  Agent Core  │
              │  agent.py    │
              └──┬───────┬───┘
                 │       │
    ┌────────────▼──┐  ┌─▼──────────────────┐
    │  RAG (ChromaDB)│  │ Self-Critique Memory│
    │  knowledge_base│  │ memory.py (ChromaDB)│
    │  4 Runbooks    │  │ Past lessons        │
    └────────────────┘  └────────────────────┘
                 │
    ┌────────────▼──────────────┐
    │   Tool Use (10 DB tools)  │
    │   tools.py                │
    └────────────┬──────────────┘
                 │
    ┌────────────▼──────────────┐
    │   Oracle DB / SQLite      │
    │   15 linked tables        │
    └───────────────────────────┘
```

### Database Schema (15 Tables)

| Table | Description |
|---|---|
| CUSTOMER | Customer master data |
| CUSTOMER_ADDRESS | Billing/service addresses |
| PRODUCT_CATALOG | Products and services offered |
| PRODUCT_DEPENDENCY | Rules and dependencies between products |
| SERVICE_SUBSCRIPTION | Customer's active subscriptions |
| ORDER_HEADER | Order master record |
| ORDER_LINE_ITEM | Individual items within an order |
| ORDER_FALLOUT | Error records for failed orders |
| BILLING_ACCOUNT | Customer billing account |
| BILLING_INVOICE | Monthly invoices |
| BILLING_CHARGE | Individual charges on invoices |
| USAGE_RECORD | Voice, data, SMS usage |
| PROVISIONING_LOG | Per-system provisioning attempt logs |
| NETWORK_ALARM | Active and historical network alarms |
| INCIDENT_TICKET | Linked incident/ticket records |

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/your-org/telco-fallout-agent.git
cd telco-fallout-agent

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env — set ANTHROPIC_API_KEY at minimum
# Leave DB_TYPE=sqlite for local development
```

### 3. Set up the database

**Option A: SQLite (local dev)**
```bash
# Generate Oracle-compatible seed SQL
python db/seed_data.py > db/seed_data.sql

# Convert and load into SQLite for local testing
python db/setup_sqlite.py      # helper script — see below
```

**Option B: Oracle (production)**
```bash
# Run DDL against your Oracle instance
sqlplus user/pass@dsn @db/oracle_ddl.sql

# Generate and run seed data
python db/seed_data.py > db/seed_data.sql
sqlplus user/pass@dsn @db/seed_data.sql
```

### 4. Ingest runbooks into ChromaDB

```bash
python -m rag.knowledge_base
```

### 5. Start the API

```bash
python -m api.main
# or
uvicorn api.main:app --reload
```

API docs available at: http://localhost:8000/docs

---

## Usage

### Resolve a fallout via API

```bash
curl -X POST http://localhost:8000/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": 42,
    "order_number": "ORD-2024042",
    "customer_id": 7,
    "error_code": "PROV_001",
    "error_message": "HLR activation timeout after 30s",
    "domain": "PROVISIONING",
    "severity": "HIGH"
  }'
```

### Example response

```json
{
  "classification": {
    "domain": "PROVISIONING",
    "issue_type": "HLR Activation Timeout",
    "severity": "HIGH"
  },
  "root_cause": "Order ORD-2024042 failed provisioning on HLR system. Provisioning logs show 3 consecutive TIMEOUT responses from HLR_SYSTEM (response code 408). An active CRITICAL network alarm ALM-8003 (NODE_DOWN) on HLR-042-NE was raised at the same time, indicating the HLR cluster in the NE region is unreachable.",
  "recommended_actions": [
    "1. Check NOC dashboard for HLR-042-NE recovery status (Alarm ALM-8003).",
    "2. Hold order ORD-2024042 — do not retry until HLR alarm clears.",
    "3. Once HLR recovers, trigger manual retry via Order Management console.",
    "4. If HLR not recovered within 2 hours, escalate to Network Engineering (P1).",
    "5. Notify customer ID 7 of expected delay via SMS/email."
  ],
  "runbook_references": ["order_fallout.txt", "provisioning_failure.txt", "network_alarm.txt"],
  "confidence": "HIGH",
  "critique_quality": "good",
  "critique_lesson": null
}
```

---

## Project Structure

```
telco-fallout-agent/
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
│
├── db/
│   ├── oracle_ddl.sql       # DDL for all 15 Oracle tables
│   └── seed_data.py         # Synthetic data generator
│
├── rag/
│   ├── knowledge_base.py    # ChromaDB ingest + retrieval
│   └── runbooks/
│       ├── order_fallout.txt
│       ├── billing_issues.txt
│       ├── provisioning_failure.txt
│       └── network_alarm.txt
│
├── agent/
│   ├── agent.py             # Main orchestrator + tool-use loop
│   ├── tools.py             # 10 DB query tools for Claude
│   ├── memory.py            # Self-critique + ChromaDB memory
│   └── db.py                # Oracle / SQLite connection layer
│
├── api/
│   └── main.py              # FastAPI endpoints
│
└── tests/
    └── test_agent.py
```

---

## Agent Capabilities

The agent uses Claude's tool_use feature with 10 DB tools:

- `get_order_details` — order status, type, channel, timestamps
- `get_order_fallouts` — error codes, messages, domain, severity
- `get_provisioning_logs` — per-system provisioning attempt results
- `get_customer_info` — account type, status, credit score
- `get_customer_subscriptions` — active services, MSISDN, IMSI
- `get_billing_account` — credit limit, balance, overdue invoices
- `get_recent_invoices` — last N invoices and payment status
- `get_active_network_alarms` — active alarms by region/severity/type
- `get_incident_tickets` — existing tickets for the order/customer
- `get_product_dependencies` — required/excluded products

---

## Self-Improving Memory Loop

After every resolution, Claude critiques its own output and stores lessons in a separate ChromaDB collection. On future similar issues, those lessons are automatically retrieved and injected into the system prompt, making the agent progressively smarter over time.

To view memory stats:
```bash
curl http://localhost:8000/memory/stats
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | — | Required |
| `CLAUDE_MODEL` | `claude-opus-4-6` | Model to use |
| `DB_TYPE` | `sqlite` | `sqlite` or `oracle` |
| `SQLITE_PATH` | `telco_dev.db` | SQLite file path |
| `ORA_USER` | — | Oracle username |
| `ORA_PASSWORD` | — | Oracle password |
| `ORA_DSN` | — | e.g. `host:1521/XEPDB1` |
