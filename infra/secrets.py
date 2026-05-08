"""
Secret Manager abstraction — Phase 12.

Provides a unified interface for retrieving secrets regardless of where they live:

  Priority order (first hit wins):
    1. AWS Secrets Manager         (when AWS_REGION + SM_SECRET_ID are set)
    2. Azure Key Vault             (when AZURE_KEY_VAULT_URL is set)
    3. Local environment variable  (always the fallback)

Usage
-----
  from infra.secrets import get_secret, get_kafka_ssl_context, get_mongo_tls_options

  kafka_password = get_secret("KAFKA_SASL_PASSWORD")
  ssl_ctx        = get_kafka_ssl_context()       # returns ssl.SSLContext or None
  mongo_opts     = get_mongo_tls_options()       # dict merged into MongoClient kwargs

mTLS Configuration
------------------
Kafka (mTLS + SASL/SCRAM-SHA-512):
  Set KAFKA_SECURITY_PROTOCOL=SSL and provide cert paths or SM secret names:
    KAFKA_SSL_CA_CERT        path or SM key for CA cert PEM
    KAFKA_SSL_CLIENT_CERT    path or SM key for client cert PEM
    KAFKA_SSL_CLIENT_KEY     path or SM key for client private key PEM

MongoDB TLS:
  Set MONGO_TLS=true and provide:
    MONGO_TLS_CA_CERT        path or SM key for CA cert PEM
    MONGO_TLS_CLIENT_CERT    path or SM key for client cert+key PEM (combined)

Environment-variable-only mode (local dev / CI):
  Do not set AWS_REGION or AZURE_KEY_VAULT_URL.  Secrets fall back to env vars
  directly — .env files are loaded by Docker Compose in the normal way.
"""

from __future__ import annotations

import logging
import os
import ssl
import tempfile
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

# ── Backend detection ─────────────────────────────────────────────────────────

_AWS_REGION    = os.getenv("AWS_REGION")
_SM_SECRET_ID  = os.getenv("SM_SECRET_ID", "synapse/production")
_AKV_URL       = os.getenv("AZURE_KEY_VAULT_URL")


@lru_cache(maxsize=256)
def get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Retrieve a secret value.  Results are cached for the process lifetime.
    Pass cache_clear=True via get_secret.cache_clear() to flush in tests.
    """
    # 1. AWS Secrets Manager
    if _AWS_REGION and _SM_SECRET_ID:
        value = _get_from_aws(key)
        if value is not None:
            return value

    # 2. Azure Key Vault
    if _AKV_URL:
        value = _get_from_akv(key)
        if value is not None:
            return value

    # 3. Environment variable fallback
    return os.getenv(key, default)


def _get_from_aws(key: str) -> Optional[str]:
    try:
        import boto3
        import json as _json
        client = boto3.client("secretsmanager", region_name=_AWS_REGION)
        response = client.get_secret_value(SecretId=_SM_SECRET_ID)
        blob = response.get("SecretString") or ""
        secrets = _json.loads(blob) if blob.startswith("{") else {}
        return secrets.get(key)
    except Exception as exc:
        logger.debug("AWS Secrets Manager lookup for %s failed: %s", key, exc)
        return None


def _get_from_akv(key: str) -> Optional[str]:
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=_AKV_URL, credential=credential)
        # Key Vault names use hyphens; translate underscores to hyphens
        secret = client.get_secret(key.replace("_", "-"))
        return secret.value
    except Exception as exc:
        logger.debug("Azure Key Vault lookup for %s failed: %s", key, exc)
        return None


# ── mTLS helpers ──────────────────────────────────────────────────────────────

def _write_pem_temp(pem_value: str, suffix: str = ".pem") -> str:
    """Write a PEM string to a temp file and return its path."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=suffix, delete=False
    ) as f:
        f.write(pem_value)
        return f.name


def _resolve_cert(env_key: str) -> Optional[str]:
    """
    If env_key points to an existing file path, return it as-is.
    If it looks like a PEM block, write it to a temp file and return the path.
    Falls back to None if unset.
    """
    value = get_secret(env_key)
    if not value:
        return None
    if os.path.isfile(value):
        return value
    if "-----BEGIN" in value:
        return _write_pem_temp(value)
    logger.warning("Secret %s does not look like a file path or PEM — skipping", env_key)
    return None


@lru_cache(maxsize=1)
def get_kafka_ssl_context() -> Optional[ssl.SSLContext]:
    """
    Build an ssl.SSLContext for Kafka mTLS.
    Returns None when KAFKA_SECURITY_PROTOCOL is not 'SSL'.

    Required environment (or Secrets Manager keys):
      KAFKA_SECURITY_PROTOCOL = SSL
      KAFKA_SSL_CA_CERT        CA certificate PEM path or inline PEM
      KAFKA_SSL_CLIENT_CERT    Client certificate PEM path or inline PEM
      KAFKA_SSL_CLIENT_KEY     Client private key PEM path or inline PEM
    """
    protocol = get_secret("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    if protocol != "SSL":
        return None

    ca_cert     = _resolve_cert("KAFKA_SSL_CA_CERT")
    client_cert = _resolve_cert("KAFKA_SSL_CLIENT_CERT")
    client_key  = _resolve_cert("KAFKA_SSL_CLIENT_KEY")

    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.check_hostname = True
    ctx.verify_mode    = ssl.CERT_REQUIRED

    if ca_cert:
        ctx.load_verify_locations(cafile=ca_cert)

    if client_cert and client_key:
        ctx.load_cert_chain(certfile=client_cert, keyfile=client_key)
    elif client_cert:
        # Combined cert+key file
        ctx.load_cert_chain(certfile=client_cert)

    logger.info("Kafka SSL context built (mTLS=%s)", bool(client_cert))
    return ctx


@lru_cache(maxsize=1)
def get_mongo_tls_options() -> dict:
    """
    Return a dict of TLS kwargs for pymongo.MongoClient / motor.AsyncIOMotorClient.
    Returns {} when MONGO_TLS is not 'true'.

    Required environment (or Secrets Manager keys):
      MONGO_TLS          = true
      MONGO_TLS_CA_CERT  CA certificate PEM path or inline PEM
      MONGO_TLS_CLIENT_CERT  client cert + key PEM path or inline PEM (combined)
    """
    if get_secret("MONGO_TLS", "false").lower() != "true":
        return {}

    opts: dict = {"tls": True}

    ca = _resolve_cert("MONGO_TLS_CA_CERT")
    if ca:
        opts["tlsCAFile"] = ca

    client_cert = _resolve_cert("MONGO_TLS_CLIENT_CERT")
    if client_cert:
        opts["tlsCertificateKeyFile"] = client_cert

    allow_invalid = get_secret("MONGO_TLS_ALLOW_INVALID_CERTS", "false")
    if allow_invalid.lower() == "true":
        opts["tlsAllowInvalidCertificates"] = True
        logger.warning("MongoDB TLS: tlsAllowInvalidCertificates=true (non-prod only)")

    logger.info("MongoDB TLS options built: %s", list(opts.keys()))
    return opts


# ── Convenience: get all Kafka bootstrap config ──────────────────────────────

def get_kafka_config() -> dict:
    """
    Return a complete aiokafka-compatible config dict with credentials and TLS.
    Merges SASL/SCRAM credentials when KAFKA_SASL_USERNAME is set.
    """
    bootstrap = get_secret("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    cfg: dict = {"bootstrap_servers": bootstrap}

    ssl_ctx = get_kafka_ssl_context()
    if ssl_ctx:
        cfg["ssl_context"]          = ssl_ctx
        cfg["security_protocol"]    = "SSL"

    sasl_user = get_secret("KAFKA_SASL_USERNAME")
    sasl_pass = get_secret("KAFKA_SASL_PASSWORD")
    if sasl_user and sasl_pass:
        cfg["sasl_mechanism"]    = get_secret("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
        cfg["sasl_plain_username"] = sasl_user
        cfg["sasl_plain_password"] = sasl_pass
        if ssl_ctx:
            cfg["security_protocol"] = "SASL_SSL"
        else:
            cfg["security_protocol"] = "SASL_PLAINTEXT"

    return cfg
