"""
RAG Knowledge Base — ChromaDB-backed runbook retrieval.
Ingests all runbook .txt files and exposes a retrieve() function
used by the agent during root cause analysis.
"""

import os
import chromadb
from chromadb.utils import embedding_functions

RUNBOOKS_DIR  = os.path.join(os.path.dirname(__file__), "runbooks")
CHROMA_PATH   = os.getenv(
    "RAG_CHROMA_PATH",
    os.path.join(os.path.dirname(__file__), ".chromadb"),
)
COLLECTION    = "telco_runbooks"


def _get_collection():
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    ef     = embedding_functions.DefaultEmbeddingFunction()
    return client.get_or_create_collection(COLLECTION, embedding_function=ef)


def ingest_runbooks(force: bool = False) -> int:
    """
    Load all .txt files from the runbooks directory into ChromaDB.
    Chunks each file into paragraphs for finer-grained retrieval.
    Set force=True to re-ingest even if already present.
    Returns the number of chunks ingested.
    """
    col = _get_collection()

    if not force and col.count() > 0:
        print(f"[RAG] Collection already has {col.count()} chunks. Skipping ingest.")
        return col.count()

    documents, metadatas, ids = [], [], []
    chunk_id = 0

    for fname in os.listdir(RUNBOOKS_DIR):
        if not fname.endswith(".txt"):
            continue
        fpath = os.path.join(RUNBOOKS_DIR, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            text = f.read()

        # Split on double newlines to get paragraph-level chunks
        paragraphs = [p.strip() for p in text.split("\n\n") if len(p.strip()) > 40]
        for para in paragraphs:
            documents.append(para)
            metadatas.append({"source": fname, "file": fpath})
            ids.append(f"chunk_{chunk_id}")
            chunk_id += 1

    if documents:
        col.upsert(documents=documents, metadatas=metadatas, ids=ids)
        print(f"[RAG] Ingested {len(documents)} chunks from {RUNBOOKS_DIR}")

    return len(documents)


def retrieve(query: str, top_k: int = 4) -> list[dict]:
    """
    Retrieve the most relevant runbook chunks for a given query.

    Returns:
        List of dicts: [{"text": str, "source": str}, ...]
    """
    col     = _get_collection()
    results = col.query(query_texts=[query], n_results=top_k)

    chunks = []
    for doc, meta in zip(results["documents"][0], results["metadatas"][0]):
        chunks.append({"text": doc, "source": meta.get("source", "unknown")})
    return chunks


def format_context(chunks: list[dict]) -> str:
    """Format retrieved chunks into a context block for the agent prompt."""
    if not chunks:
        return ""
    parts = ["=== RELEVANT RUNBOOK CONTEXT ==="]
    for chunk in chunks:
        parts.append(f"[{chunk['source']}]\n{chunk['text']}")
    parts.append("=== END RUNBOOK CONTEXT ===")
    return "\n\n".join(parts)


if __name__ == "__main__":
    ingest_runbooks(force=True)
    results = retrieve("OCS quota assignment failed provisioning error")
    for r in results:
        print(f"\n--- {r['source']} ---\n{r['text'][:300]}")
