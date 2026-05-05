#!/usr/bin/env bash
# Full Workload Identity + AKV wiring for the Telco Agent.
#
# Run from the repo root with your Anthropic key in the environment:
#   set ANTHROPIC_API_KEY=sk-ant-...
#   bash infra/akv-workload-identity-setup.sh
#
# Safe to re-run: every step is idempotent.
set -euo pipefail

# ──── CONFIG ──────────────────────────────────────────────────────────────────
RG="rg-telco-agent"
KV="kv-telco-agent"
AKS="aks-telco-agent"
LOCATION="westeurope"
MI_DEV="mi-telco-agent-dev"
MI_PROD="mi-telco-agent-prod"
NS_DEV="telco-agent-dev"
NS_PROD="telco-agent-prod"
SA_NAME="telco-agent-workload"
DEV_API_KEY="G_jdt1aom16o_bGSdV9MT6zCKPY47xzmff5guB_GJ5k"
PROD_API_KEY="x0IwfTX5LKjk2Rmy8gRgJgqyd1eAniMB_MMi5PSC4e4"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEV_YAML="${REPO_ROOT}/k8s/overlays/dev/secret-provider.yaml"
PROD_YAML="${REPO_ROOT}/k8s/overlays/prod/secret-provider.yaml"
# ──────────────────────────────────────────────────────────────────────────────

echo "▶ Verifying az login..."
az account show -o none 2>/dev/null || { echo "ERROR: Run 'az login' first."; exit 1; }
TENANT_ID=$(az account show --query tenantId -o tsv)
echo "  Tenant: ${TENANT_ID}"

# ── 1. AKS: ensure OIDC issuer + Workload Identity are enabled ────────────────
echo ""
echo "▶ Checking AKS OIDC + Workload Identity..."
OIDC_ON=$(az aks show -n "$AKS" -g "$RG" --query "oidcIssuerProfile.enabled" -o tsv 2>/dev/null || echo "false")
WI_ON=$(az aks show   -n "$AKS" -g "$RG" --query "securityProfile.workloadIdentity.enabled" -o tsv 2>/dev/null || echo "false")

if [[ "$OIDC_ON" != "true" || "$WI_ON" != "true" ]]; then
  echo "  Enabling OIDC issuer + Workload Identity on AKS (takes ~2 min)..."
  az aks update -n "$AKS" -g "$RG" \
    --enable-oidc-issuer \
    --enable-workload-identity \
    -o none
fi

OIDC_ISSUER=$(az aks show -n "$AKS" -g "$RG" --query "oidcIssuerProfile.issuerUrl" -o tsv)
echo "  ✓ OIDC issuer: ${OIDC_ISSUER}"

# ── 2. Create managed identities (idempotent) ─────────────────────────────────
echo ""
echo "▶ Creating managed identities..."
az identity create -n "$MI_DEV"  -g "$RG" -l "$LOCATION" -o none
az identity create -n "$MI_PROD" -g "$RG" -l "$LOCATION" -o none
echo "  ✓ ${MI_DEV}, ${MI_PROD}"

DEV_CLIENT_ID=$(az identity show    -n "$MI_DEV"  -g "$RG" --query clientId    -o tsv)
DEV_PRINCIPAL_ID=$(az identity show -n "$MI_DEV"  -g "$RG" --query principalId -o tsv)
PROD_CLIENT_ID=$(az identity show   -n "$MI_PROD" -g "$RG" --query clientId    -o tsv)
PROD_PRINCIPAL_ID=$(az identity show -n "$MI_PROD" -g "$RG" --query principalId -o tsv)

echo "  Dev  clientId: ${DEV_CLIENT_ID}"
echo "  Prod clientId: ${PROD_CLIENT_ID}"

# ── 3. Federated credentials (delete+recreate so re-runs don't error) ─────────
echo ""
echo "▶ Wiring federated identity credentials..."

for env_name in dev prod; do
  MI_NAME="mi-telco-agent-${env_name}"
  CRED_NAME="telco-agent-${env_name}-aks"
  NS="telco-agent-${env_name}"
  az identity federated-credential delete \
    --name "$CRED_NAME" --identity-name "$MI_NAME" --resource-group "$RG" \
    --yes -o none 2>/dev/null || true
  az identity federated-credential create \
    --name "$CRED_NAME" --identity-name "$MI_NAME" --resource-group "$RG" \
    --issuer "$OIDC_ISSUER" \
    --subject "system:serviceaccount:${NS}:${SA_NAME}" \
    --audience "api://AzureADTokenExchange" -o none
  echo "  ✓ ${env_name}: system:serviceaccount:${NS}:${SA_NAME}"
done

# ── 4. Create Key Vault if missing, then grant access ─────────────────────────
echo ""
echo "▶ Ensuring Key Vault exists..."
KV_EXISTS=$(az keyvault list -g "$RG" --query "[?name=='${KV}'].name" -o tsv)
if [[ -z "$KV_EXISTS" ]]; then
  echo "  Creating ${KV} (RBAC-authorised)..."
  az keyvault create \
    --name "$KV" \
    --resource-group "$RG" \
    --location "$LOCATION" \
    --enable-rbac-authorization true \
    -o none
  echo "  ✓ Key Vault created."
else
  echo "  ✓ ${KV} already exists."
fi

echo "▶ Granting Key Vault access..."
KV_ID=$(az keyvault show -n "$KV" -g "$RG" --query id -o tsv)
KV_RBAC=$(az keyvault show -n "$KV" -g "$RG" --query properties.enableRbacAuthorization -o tsv)

if [[ "$KV_RBAC" == "true" ]]; then
  for pid in "$DEV_PRINCIPAL_ID" "$PROD_PRINCIPAL_ID"; do
    # Not using || true — a silent failure here causes the pod to get 403 at mount time.
    az role assignment create \
      --role "Key Vault Secrets User" \
      --assignee-object-id "$pid" \
      --assignee-principal-type ServicePrincipal \
      --scope "$KV_ID" -o none
  done
  echo "  ✓ RBAC: 'Key Vault Secrets User' granted to both MIs"
else
  az keyvault set-policy -n "$KV" --object-id "$DEV_PRINCIPAL_ID"  --secret-permissions get list -o none
  az keyvault set-policy -n "$KV" --object-id "$PROD_PRINCIPAL_ID" --secret-permissions get list -o none
  echo "  ✓ Access policies: get+list granted to both MIs"
fi

# ── 5. Grant current user write access so the script can set secrets ──────────
echo ""
echo "▶ Granting current user Key Vault Secrets Officer..."
CURRENT_USER_OID=$(az ad signed-in-user show --query id -o tsv)
echo "  Current user OID: ${CURRENT_USER_OID}"
# Not using || true here — we want to see this error if it fails.
az role assignment create \
  --role "Key Vault Secrets Officer" \
  --assignee-object-id "$CURRENT_USER_OID" \
  --assignee-principal-type User \
  --scope "$KV_ID" -o none
echo "  ✓ Secrets Officer granted."
# RBAC on a freshly created RBAC-authorised vault can take up to 5 min to
# propagate globally. 90s covers the vast majority of cases.
echo "  Waiting 90s for RBAC to propagate..."
sleep 90

# ── 6. Write secrets to AKV ───────────────────────────────────────────────────
echo ""
echo "▶ Writing secrets to ${KV}..."
if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
  read -rsp "  Enter ANTHROPIC_API_KEY (input hidden): " ANTHROPIC_API_KEY
  echo
fi

az keyvault secret set --vault-name "$KV" --name "ANTHROPIC-API-KEY"          --value "$ANTHROPIC_API_KEY" -o none
az keyvault secret set --vault-name "$KV" --name "TELCO-AGENT-API-KEYS-DEV"   --value "$DEV_API_KEY"       -o none
az keyvault secret set --vault-name "$KV" --name "TELCO-AGENT-API-KEYS-PROD"  --value "$PROD_API_KEY"      -o none
echo "  ✓ ANTHROPIC-API-KEY, TELCO-AGENT-API-KEYS-DEV, TELCO-AGENT-API-KEYS-PROD"

# ── 7. Patch PLACEHOLDER values in overlay YAML files ─────────────────────────
echo ""
echo "▶ Patching secret-provider.yaml files..."
sed -i \
  -e "s/PLACEHOLDER_TENANT_ID/${TENANT_ID}/g" \
  -e "s/PLACEHOLDER_DEV_MI_CLIENT_ID/${DEV_CLIENT_ID}/g" \
  "$DEV_YAML"

sed -i \
  -e "s/PLACEHOLDER_TENANT_ID/${TENANT_ID}/g" \
  -e "s/PLACEHOLDER_PROD_MI_CLIENT_ID/${PROD_CLIENT_ID}/g" \
  "$PROD_YAML"

if grep -r "PLACEHOLDER_" "${REPO_ROOT}/k8s/overlays/" 2>/dev/null; then
  echo "ERROR: Some PLACEHOLDER values were not replaced."
  exit 1
fi
echo "  ✓ All placeholders resolved."

# ── 7. Summary ────────────────────────────────────────────────────────────────
cat <<EOF

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 Setup complete.

 Commit and deploy:

   git add k8s/overlays/dev/secret-provider.yaml \\
           k8s/overlays/prod/secret-provider.yaml
   git commit -m "feat(akv): wire Workload Identity client IDs"
   git push

   gh workflow run deploy-aks.yml -f environment=dev -f image_tag=latest

 Then verify:
   curl -s -o /dev/null -w "%{http_code}" -X POST https://dev.42-ds.net/resolve
   # → 401

   curl -s -o /dev/null -w "%{http_code}" -X POST https://dev.42-ds.net/resolve \\
     -H "X-API-Key: ${DEV_API_KEY}"
   # → 422 (auth OK, body missing is fine)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EOF
