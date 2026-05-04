# Deploying the Telco Fallout Agent

Three audiences:

1. **Local developer** — run the container on your laptop.
2. **Anyone on your team** — pull the prebuilt image from GHCR and run it.
3. **Production** — deploy to Azure Container Apps via GitHub Actions.

---

## 1. Run locally with Docker Compose

```bash
# One-time: create your env file
cp .env.example .env
# Edit .env and set ANTHROPIC_API_KEY=sk-ant-...

# Build + start (auto-seeds SQLite and ingests runbooks on first boot)
docker compose up --build

# Visit:
#   http://localhost:8000/docs    — Swagger UI
#   http://localhost:8000/health  — Health check
```

State (SQLite DB, ChromaDB collections, agent memory) persists in the named
volume `telco_data`. To start completely fresh:

```bash
docker compose down -v
```

---

## 2. Build and push the image manually

```bash
# Build
docker build -t telco-agent:dev .

# Smoke-test
docker run --rm -p 8000:8000 --env-file .env telco-agent:dev

# Tag and push to GitHub Container Registry
docker tag telco-agent:dev ghcr.io/<your-org>/<your-repo>:latest
echo "$GHCR_PAT" | docker login ghcr.io -u <github-user> --password-stdin
docker push ghcr.io/<your-org>/<your-repo>:latest
```

`GHCR_PAT` is a GitHub Personal Access Token with `write:packages` scope.

---

## 3. CI/CD via GitHub Actions

Two workflows live in `.github/workflows/`:

| File | Trigger | What it does |
|---|---|---|
| `ci.yml` | every push + PR to `main` | Lints, runs `pytest`, then on `main` builds the container and pushes `ghcr.io/<repo>:latest` and `:sha-<short>` |
| `deploy-azure.yml` | manual (`workflow_dispatch`) | Updates an existing Azure Container App with the chosen image tag |

### One-time GitHub setup
1. Push the repo to GitHub.
2. Settings → Actions → General → **Read and write permissions** for `GITHUB_TOKEN`.
3. Settings → Packages → make the GHCR package public (or grant your Azure
   Container App pull access via a PAT).

### One-time Azure setup
```bash
# Variables
RG=rg-telco-agent
LOC=westeurope
APP_ENV=telco-agent-env
APP=telco-agent-api
IMAGE=ghcr.io/<your-org>/<your-repo>:latest

# Resource group + Container Apps environment
az group create -n $RG -l $LOC
az containerapp env create -n $APP_ENV -g $RG -l $LOC

# First deploy (creates the app)
az containerapp create \
  --name $APP --resource-group $RG --environment $APP_ENV \
  --image $IMAGE \
  --target-port 8000 --ingress external \
  --secrets anthropic-api-key=<your-key> \
  --env-vars ANTHROPIC_API_KEY=secretref:anthropic-api-key DB_TYPE=sqlite \
  --min-replicas 1 --max-replicas 3

# Service principal for GitHub Actions
az ad sp create-for-rbac \
  --name "telco-agent-github-deploy" \
  --role contributor \
  --scopes /subscriptions/<sub-id>/resourceGroups/$RG \
  --sdk-auth
# → paste the JSON output into GitHub secret AZURE_CREDENTIALS
```

Add these GitHub secrets:

| Secret | Value |
|---|---|
| `AZURE_CREDENTIALS` | JSON output from the `az ad sp create-for-rbac` command above |
| `AZURE_RESOURCE_GROUP` | `rg-telco-agent` |
| `AZURE_CONTAINERAPP_NAME` | `telco-agent-api` |

Then go to **Actions → Deploy to Azure Container Apps → Run workflow**, pick a
tag (e.g. `latest` or `sha-abc1234`), and ship it.

---

## What's NOT production-ready yet

The container is deployable but the architecture is still single-replica
prototype-shaped. Before real traffic, address:

- [ ] **Replace SQLite** with managed Postgres (Azure DB for PostgreSQL) or
      Oracle (Oracle DB@Azure). Set `DB_TYPE=oracle` and the `ORA_*` vars.
- [ ] **Replace local ChromaDB** with a managed vector store (pgvector,
      Azure AI Search, Pinecone). Local Chroma in a single container does not
      survive horizontal scaling.
- [ ] **Auth on the API** — currently anyone who reaches `/resolve` can call it.
      Add API keys (FastAPI dependency) or Azure API Management in front.
- [ ] **Async processing** — agent calls take 5–30s. Put a queue (Service Bus)
      between the API and a worker pool so HTTP returns immediately.
- [ ] **Secrets in Key Vault**, not env vars baked into the Container App.
- [ ] **Observability** — wire Application Insights for logs + traces, and
      track Anthropic token usage per request (cost matters fast).
- [ ] **Rate limits + cost guardrails** on the Anthropic API.
