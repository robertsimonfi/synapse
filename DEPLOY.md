# Deploying the Telco Fallout Agent

Three audiences:

1. **Local developer** — run the container on your laptop.
2. **Anyone on your team** — pull the prebuilt image from GHCR and run it.
3. **Production** — deploy to **Azure Kubernetes Service (AKS)** with separate
   `dev` and `prod` environments, via GitHub Actions.

---

## 1. Run locally with Docker Compose

```bash
cp .env.example .env
# edit .env: set ANTHROPIC_API_KEY=sk-ant-...

docker compose up --build
# → http://localhost:8000/docs
```

State persists in the named volume `telco_data`. Wipe with `docker compose down -v`.

---

## 2. Pull and run the prebuilt image

```bash
docker login ghcr.io   # username = your GitHub handle, password = a PAT with read:packages
docker pull ghcr.io/robertsimonfi/telco-agent:latest
docker run -p 8000:8000 -e ANTHROPIC_API_KEY=sk-ant-... ghcr.io/robertsimonfi/telco-agent:latest
```

---

## 3. CI/CD pipelines

| File | Trigger | What it does |
|---|---|---|
| `.github/workflows/ci.yml` | push/PR to `main` | Lint, test, build container, push to **GHCR**. On `main`, also fires the dev deploy. |
| `.github/workflows/deploy-aks.yml` | manual + auto-from-CI | Imports image GHCR→ACR, applies Kustomize overlay to selected namespace. Prod gated by GitHub Environment approval. |

Flow on every push to `main`:

```
push → ci.yml ──▶ lint+test ──▶ build & push GHCR ──▶ trigger deploy-aks(dev)
                                                              │
                                                              ▼
                                                    auto-deploy to telco-agent-dev
                                                              │
                                                  (you click "Run workflow" → prod
                                                   in Actions tab when ready)
                                                              ▼
                                                  approval gate (GitHub Env)
                                                              ▼
                                                    deploy to telco-agent-prod
```

---

## 4. One-time Azure setup

### Prerequisites on your laptop

| Tool | Install |
|---|---|
| Azure CLI | `winget install --id Microsoft.AzureCLI` |
| `kubectl` | `az aks install-cli` (after `az` is installed) |
| `helm` | `winget install --id Helm.Helm` |

Then:

```bash
az login                                 # opens browser
az account set --subscription <name|id>  # if you have multiple subs

# From the repo root, run the setup script (creates RG, ACR, AKS, ingress, SP):
bash infra/azure-setup.sh
```

The script provisions everything (~7 min) and prints **all the values you need
to paste into GitHub Secrets**:

| Secret | Source |
|---|---|
| `AZURE_CREDENTIALS` | full JSON block from script output |
| `AZURE_RESOURCE_GROUP` | `rg-telco-agent` (or whatever you set) |
| `AKS_CLUSTER_NAME` | `aks-telco-agent` |
| `ACR_NAME` | the unique name (script generates one) |
| `ACR_LOGIN_SERVER` | `<acr_name>.azurecr.io` |
| `ANTHROPIC_API_KEY` | your existing key (same as `.env`) |

Add them at: **Settings → Secrets and variables → Actions → New repository secret**.

### Create GitHub Environments (for prod approval gate)

**Settings → Environments**:

1. **New environment** → name `dev` → save (no protection rules — auto-deploys)
2. **New environment** → name `prod` → check **Required reviewers** → add yourself → save

Now the prod deploy workflow will pause and wait for your approval click.

---

## 5. First deploy

After the setup script finishes and secrets are in place:

```
Actions tab → "Deploy to AKS" → Run workflow
   environment: dev
   image_tag:   latest   (or sha-<commit>)
```

About 1–2 min later, your dev API is live at:

```
http://dev.<LB_IP>.nip.io/docs       (Swagger UI)
http://dev.<LB_IP>.nip.io/health     (healthcheck)
```

The script prints the exact URLs at the end.

For prod: same workflow, pick `environment: prod` — GitHub will prompt for
your approval before the deploy proceeds.

---

## 6. Day-to-day operations

```bash
# Get/refresh kubeconfig (after a fresh shell)
az aks get-credentials -g rg-telco-agent -n aks-telco-agent --overwrite-existing

# See what's running
kubectl get pods -n telco-agent-dev
kubectl get pods -n telco-agent-prod

# Tail logs
kubectl logs -f -n telco-agent-dev deploy/telco-agent
kubectl logs -f -n telco-agent-prod deploy/telco-agent

# Describe a problem pod
kubectl describe pod -n telco-agent-dev <pod-name>

# Restart a deployment (e.g. to re-pull a refreshed `latest`)
kubectl rollout restart -n telco-agent-dev deployment/telco-agent

# Port-forward without going through ingress
kubectl port-forward -n telco-agent-dev svc/telco-agent 8000:80
# → http://localhost:8000/docs
```

---

## 7. What's NOT production-ready yet

The cluster is up but the architecture is still single-replica prototype-shaped.
Before real production traffic, address these in roughly this order:

- [ ] **Buy a real domain + add HTTPS** — install `cert-manager` in the
      cluster, change ingress hostnames from `nip.io` to your domain, and
      requests will be auto-encrypted with Let's Encrypt certs.
- [ ] **Replace SQLite** with managed Postgres (Azure DB for PostgreSQL) or
      Oracle (Oracle DB@Azure). Set `DB_TYPE=oracle` + `ORA_*` vars in the
      ConfigMap (move the API key to a Secret).
- [ ] **Replace local ChromaDB** with a managed vector store (pgvector,
      Azure AI Search, Pinecone). Until then `replicas: 1` is enforced by the
      `Recreate` strategy + ReadWriteOnce PVC.
- [ ] **Auth on the API** — add API keys (FastAPI dependency) or put Azure
      API Management in front. Today `/resolve` is open to anyone with the URL.
- [ ] **Async processing** — agent calls take 5–30s. Add a queue (Service
      Bus) between the API and a worker pool so HTTP returns immediately.
- [ ] **Move secrets to Azure Key Vault** + use the AKS CSI Secret Store
      driver instead of plain Kubernetes Secrets.
- [ ] **Observability** — install the Azure Monitor for Containers add-on
      (`az aks enable-addons -a monitoring ...`) and track Anthropic token
      usage per request.
- [ ] **Rate limiting + cost guardrails** on the Anthropic API.

---

## 8. Tearing it all down

If you want to delete every Azure resource (and stop billing):

```bash
az group delete -n rg-telco-agent --yes --no-wait
```

This nukes the AKS cluster, ACR, ingress LB, disks, and the resource group itself.
The service principal lives at the directory level — delete separately:

```bash
az ad sp delete --id $(az ad sp list --display-name sp-telco-agent-github --query '[0].id' -o tsv)
```
