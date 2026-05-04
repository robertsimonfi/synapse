"""
Self-critique memory loop — ChromaDB-backed agent memory.
After each resolution, the agent critiques its own output and stores
lessons learned. Future similar issues retrieve those lessons automatically.
"""

import json
import os
from datetime import datetime

import chromadb
from chromadb.utils import embedding_functions
import anthropic

CHROMA_PATH = os.getenv(
    "AGENT_MEMORY_PATH",
    os.path.join(os.path.dirname(__file__), ".agent_memory"),
)
COLLECTION  = "agent_reflections"

_client_cache = None


def _get_collection():
    global _client_cache
    if _client_cache is None:
        _client_cache = chromadb.PersistentClient(path=CHROMA_PATH)
    ef = embedding_functions.DefaultEmbeddingFunction()
    return _client_cache.get_or_create_collection(COLLECTION, embedding_function=ef)


# ── Retrieval ──────────────────────────────────────────────────────────────────

def retrieve_memories(issue_description: str, top_k: int = 3) -> list[dict]:
    """
    Retrieve the most relevant past lessons for a given issue.
    Returns list of {"lesson": str, "error_code": str, "quality": str, "timestamp": str}
    """
    col = _get_collection()
    if col.count() == 0:
        return []

    results = col.query(query_texts=[issue_description], n_results=min(top_k, col.count()))
    memories = []
    for doc, meta in zip(results["documents"][0], results["metadatas"][0]):
        memories.append({
            "lesson":     doc,
            "error_code": meta.get("error_code", ""),
            "domain":     meta.get("domain", ""),
            "quality":    meta.get("quality", ""),
            "timestamp":  meta.get("timestamp", ""),
        })
    return memories


def format_memories(memories: list[dict]) -> str:
    """Format retrieved memories for injection into the agent system prompt."""
    if not memories:
        return ""
    lines = ["=== LESSONS FROM PAST SIMILAR ISSUES ==="]
    for m in memories:
        lines.append(
            f"[{m['error_code']} / {m['domain']}] {m['lesson']}"
        )
    lines.append("=== END LESSONS ===")
    return "\n".join(lines)


# ── Self-critique & storage ────────────────────────────────────────────────────

def self_critique_and_store(
    anthropic_client: anthropic.Anthropic,
    issue_description: str,
    error_code: str,
    domain: str,
    agent_output: str,
) -> dict:
    """
    Ask Claude to critique the agent's resolution output.
    If the quality is not perfect, extract a lesson and store it in ChromaDB.

    Returns the critique dict.
    """
    critique_prompt = f"""You are reviewing an AI agent's resolution output for a Telco operational issue.

Issue: {issue_description}
Error Code: {error_code}
Domain: {domain}

Agent's output:
{agent_output}

Evaluate the quality of this response. Reply ONLY with valid JSON in this exact format:
{{
  "quality": "good" | "mediocre" | "poor",
  "issues": ["list of specific weaknesses or gaps, if any"],
  "lesson": "one concrete, actionable lesson for handling this type of issue better in the future, or null if quality was good"
}}"""

    response = anthropic_client.messages.create(
        model=os.getenv("CLAUDE_MODEL", "claude-opus-4-6"),
        max_tokens=512,
        messages=[{"role": "user", "content": critique_prompt}],
    )

    raw = response.content[0].text.strip()
    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = "\n".join(raw.split("\n")[1:-1])

    try:
        critique = json.loads(raw)
    except json.JSONDecodeError:
        critique = {"quality": "good", "issues": [], "lesson": None}

    # Only persist if there's something to learn
    if critique.get("lesson"):
        _store_memory(
            lesson=critique["lesson"],
            issue_description=issue_description,
            error_code=error_code,
            domain=domain,
            quality=critique["quality"],
        )
        print(f"[Memory] Stored lesson: {critique['lesson'][:100]}...")

    return critique


def _store_memory(
    lesson: str,
    issue_description: str,
    error_code: str,
    domain: str,
    quality: str,
):
    col       = _get_collection()
    unique_id = f"{error_code}_{datetime.utcnow().isoformat()}"
    col.upsert(
        documents=[lesson],
        metadatas=[{
            "issue_summary": issue_description[:300],
            "error_code":    error_code,
            "domain":        domain,
            "quality":       quality,
            "timestamp":     datetime.utcnow().isoformat(),
        }],
        ids=[unique_id],
    )


def memory_stats() -> dict:
    col = _get_collection()
    return {"total_memories": col.count(), "collection": COLLECTION}
