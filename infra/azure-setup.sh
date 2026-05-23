#!/usr/bin/env bash
# One-shot Azure infrastructure setup for Synapse.
# Creates: resource group, ACR, AKS cluster (with ACR pull access),
# NGINX ingress controller, and a service principal for GitHub Actions deploys.
#
# Run from a terminal where `az` is logged in:  az login
# Tested with az CLI 2.60+.
#
# Idempotent-ish: re-running will skip already-existing resources where possible,
# but ACR names must be globally unique — pick yours below before first run.
set -euo pipefail

# ──── EDIT THESE BEFORE FIRST RUN ─────────────────────────────────────────────
LOCATION="${LOCATION:-westeurope}"
RG="${RG:-rg-synapse}"
ACR_NAME="${ACR_NAME:-acrsynapse18683}"      
AKS_NAME="${AKS_NAME:-aks-synapse}"
NODE_SIZE="${NODE_SIZE:-Standard_B2s_v2}"       # <-- Updated to v2 VM size
NODE_COUNT="${NODE_COUNT:-2}"
SP_NAME="${SP_NAME:-sp-synapse-github}"
# ──────────────────────────────────────────────────────────────────────────────

echo "Using:"
echo "  Location:   $LOCATION"
echo "  RG:         $RG"
echo "  ACR:        $ACR_NAME"
echo "  AKS:        $AKS_NAME ($NODE_COUNT × $NODE_SIZE)"
echo "  SP:         $SP_NAME"
echo ""
read -p "Proceed? (y/N) " -n 1 -r
echo
[[ $REPLY =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }

SUB_ID=$(az account show --query id -o tsv)
echo "Subscription: $SUB_ID"

# 1. Resource group
echo ""
echo "▶ Creating resource group..."
az group create --name "$RG" --location "$LOCATION" -o none

# 2. Azure Container Registry
echo "▶ Creating ACR ($ACR_NAME)..."
az acr create --resource-group "$RG" --name "$ACR_NAME" --sku Basic -o none
ACR_LOGIN_SERVER=$(az acr show -n "$ACR_NAME" -g "$RG" --query loginServer -o tsv)
echo "  ACR login server: $ACR_LOGIN_SERVER"

# 3. AKS cluster (with managed identity, attached to ACR for image pulls)
echo "▶ Creating AKS cluster ($AKS_NAME) — takes ~5 min..."
# az aks create \
#   --resource-group "$RG" \
#   --name "$AKS_NAME" \
#   --node-count "$NODE_COUNT" \
#   --node-vm-size "$NODE_SIZE" \
#   --enable-managed-identity \
#   --attach-acr "$ACR_NAME" \
#   --generate-ssh-keys \
#   --location "$LOCATION" \
#   -o none

# 4. Pull kubeconfig
echo "▶ Fetching kubeconfig..."
az aks get-credentials --resource-group "$RG" --name "$AKS_NAME" --overwrite-existing
kubectl get nodes

# 5. Install NGINX ingress controller via Helm
echo "▶ Installing NGINX ingress controller..."
if ! command -v helm >/dev/null 2>&1; then
  echo "  helm not found — install it first: https://helm.sh/docs/intro/install/"
  exit 1
fi
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx >/dev/null 2>&1 || true
helm repo update >/dev/null
# NOTE: The annotation below is REQUIRED on AKS. Without it, the Azure Load
# Balancer probes "/" (default) and gets a 404 from NGINX, marks all backends
# unhealthy, and silently drops every external connection. With it, the LB
# probes "/healthz" (NGINX's built-in liveness endpoint) and routing works.
helm upgrade --install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx --create-namespace \
  --set controller.service.type=LoadBalancer \
  --set "controller.service.annotations.service\.beta\.kubernetes\.io/azure-load-balancer-health-probe-request-path=/healthz" \
  --wait --timeout 5m

# 5b. Install cert-manager (auto-renewed Let's Encrypt certs for ingress hosts).
echo "▶ Installing cert-manager..."
helm repo add jetstack https://charts.jetstack.io >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true \
  --version v1.16.1 \
  --wait --timeout 5m

echo "▶ Applying ClusterIssuers (Let's Encrypt staging + prod)..."
kubectl apply -f "$(dirname "$0")/cluster-issuers.yaml"

echo ""
echo "▶ Waiting for LoadBalancer IP (this can take a minute)..."
for i in {1..30}; do
  LB_IP=$(kubectl -n ingress-nginx get svc ingress-nginx-controller \
            -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
  [[ -n "$LB_IP" ]] && break
  sleep 5
done
[[ -n "$LB_IP" ]] || { echo "Timed out waiting for LB IP."; exit 1; }
echo "  LoadBalancer IP: $LB_IP"

# 6. Service principal for GitHub Actions
echo ""
echo "▶ Creating service principal for GitHub Actions..."
SP_JSON=$(MSYS_NO_PATHCONV=1 az ad sp create-for-rbac \
  --name "$SP_NAME" \
  --role contributor \
  --scopes "/subscriptions/$SUB_ID/resourceGroups/$RG" \
  --sdk-auth)

# 7. Print everything the user needs to paste into GitHub Secrets
cat <<EOF

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SUCCESS — Azure infrastructure is ready.

 Set the following GitHub Actions secrets in your repo:
   Settings → Secrets and variables → Actions → New repository secret
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

 AZURE_CREDENTIALS:
$SP_JSON

 AZURE_RESOURCE_GROUP:
   $RG

 AKS_CLUSTER_NAME:
   $AKS_NAME

 ACR_NAME:
   $ACR_NAME

 ACR_LOGIN_SERVER:
   $ACR_LOGIN_SERVER

 LB_IP (the cluster's NGINX ingress public IP — used for nip.io hostnames):
   $LB_IP

 ANTHROPIC_API_KEY:
   <your existing Anthropic API key — same as in .env>

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 Once secrets are set, also create the GitHub Environments:
   Settings → Environments → New environment → "dev"   (no protection rules)
   Settings → Environments → New environment → "prod"  (require reviewer = you)

 Then trigger the deploy:
   Actions → "Deploy to AKS" → Run workflow → environment=dev
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

 Your URLs after first deploy:
   Dev:  http://dev.$LB_IP.nip.io/docs
   Prod: http://prod.$LB_IP.nip.io/docs

EOF
