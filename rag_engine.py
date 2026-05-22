"""
RAG engine for session transcripts.

Embeds conversation turns via mlx-embeddings. Stores vectors in Milvus — either
a remote Standalone instance (via SESSIONFLOW_MILVUS_URI) or embedded Milvus Lite
at ~/.sessionflow/milvus.db (fallback).

Full-text search via SQLite FTS5 sidecar for hybrid search (vector + keyword).
Results merged with Reciprocal Rank Fusion (RRF).

Each turn is tagged with a project_root field, enabling per-project or cross-project search.

Supports multiple embedding models via SESSIONFLOW_MODEL env var (default: embeddinggemma).
"""

import hashlib
import json
import os
from pathlib import Path

# Block all HuggingFace network access at runtime.
# Models must be pre-downloaded via setup.sh / download-model.sh.
os.environ['HF_HUB_OFFLINE'] = '1'
os.environ['TRANSFORMERS_OFFLINE'] = '1'

from pymilvus import MilvusClient, DataType, CollectionSchema, FieldSchema
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator, List, Dict, Optional
import asyncio
import logging
import sys
import time

from fts_hybrid import FTSIndex, rrf_merge
from embedding_control import EmbeddingIdentity, get_embedding_budget
from provider_adapters import (
    LEGAL_PROVIDERS,
    LEGAL_SOURCE_KINDS,
    default_provider_metadata,
)

logger = logging.getLogger("sessionflow.milvus")


def _is_remote_uri(uri: str) -> bool:
    """True when uri points to a remote Milvus Standalone (http:// or https://)."""
    return uri.startswith("http://") or uri.startswith("https://")


# --- Model registry ---

_MODEL_REGISTRY = {
    "modernbert": {
        "model_id": "nomic-ai/modernbert-embed-base",
        "embed_dim": 768,
        "max_tokens": 8192,
        "search_prefix": "search_query: ",
        "document_prefix": "search_document: ",
        "cache_subdir": "models--nomic-ai--modernbert-embed-base",
    },
    "embeddinggemma": {
        "model_id": "mlx-community/embeddinggemma-300m-bf16",
        "embed_dim": 768,
        "max_tokens": 2048,
        "search_prefix": "task: search result | query: ",
        "document_prefix": "title: none | text: ",
        "cache_subdir": "models--mlx-community--embeddinggemma-300m-bf16",
    },
}

_MODEL_NAME = os.getenv("SESSIONFLOW_MODEL", "embeddinggemma").lower()
if _MODEL_NAME not in _MODEL_REGISTRY:
    raise ValueError(
        f"Unknown model '{_MODEL_NAME}'. "
        f"Valid options: {', '.join(_MODEL_REGISTRY.keys())}"
    )

_MODEL_CFG = _MODEL_REGISTRY[_MODEL_NAME]
_EMBED_DIM = _MODEL_CFG["embed_dim"]
_MODEL_ID = _MODEL_CFG["model_id"]
_MODEL_CACHE = Path.home() / ".cache/huggingface/hub" / _MODEL_CFG["cache_subdir"]
_SEARCH_PREFIX = _MODEL_CFG["search_prefix"]
_DOCUMENT_PREFIX = _MODEL_CFG["document_prefix"]

COLLECTION_NAME = "sessions"

# --- Model identity check ---

_IDENTITY_FILE = Path.home() / ".sessionflow" / "model_identity.json"


def _check_model_identity(db_path: Optional[str] = None):
    """Verify that the active model matches what was used to build the index.

    On first run, stamps model_identity.json. On subsequent runs, if the stored
    model differs and the index has data, raises an error to prevent mixing
    incompatible vectors.
    """
    _IDENTITY_FILE.parent.mkdir(parents=True, exist_ok=True)

    if _IDENTITY_FILE.exists():
        stored = json.loads(_IDENTITY_FILE.read_text())
        stored_model = stored.get("model_name", "")
        if stored_model and stored_model != _MODEL_NAME:
            # Check if the index actually has data before raising
            has_data = False
            if db_path:
                try:
                    client = MilvusClient(db_path)
                    if client.has_collection(COLLECTION_NAME):
                        count = client.query(
                            collection_name=COLLECTION_NAME,
                            filter="",
                            limit=1,
                            output_fields=["id"],
                        )
                        has_data = len(count) > 0
                    client.close()
                except Exception:
                    pass
            if has_data:
                raise RuntimeError(
                    f"Model mismatch: index was built with '{stored_model}' but "
                    f"SESSIONFLOW_MODEL is '{_MODEL_NAME}'. "
                    f"Run cleanup.py reset or clear the index before switching models."
                )
            # Index is empty — safe to overwrite the stamp
    # Stamp current model
    _IDENTITY_FILE.write_text(json.dumps({"model_name": _MODEL_NAME}))


def get_model_name() -> str:
    """Return the active model's short name (e.g. 'modernbert', 'embeddinggemma')."""
    return _MODEL_NAME


def get_embedding_identity() -> Dict[str, object]:
    """Return the active local embedding identity for health/status output."""
    try:
        identity = EmbeddingIdentity.current_local()
    except ValueError as exc:
        logger.warning("Invalid embedding identity: %s", exc)
        return {
            "embedding_provider": "unknown",
            "model_name": "unknown",
            "dimension": None,
            "collection_name": COLLECTION_NAME,
            "created_at": "",
            "error": str(exc),
        }
    return {
        "embedding_provider": identity.embedding_provider,
        "model_name": identity.model_name,
        "dimension": identity.dimension,
        "collection_name": identity.collection_name,
        "created_at": identity.created_at,
    }

_mlx_model = None
_mlx_tokenizer = None
_mlx_load = None
_mlx_generate = None
_mlx_core = None


def _load_mlx_runtime():
    """Import MLX lazily so non-embedding tests/status paths cannot crash at import time."""
    global _mlx_load, _mlx_generate, _mlx_core
    if _mlx_load is None or _mlx_generate is None or _mlx_core is None:
        from mlx_embeddings.utils import load as mlx_load, generate as mlx_generate
        import mlx.core as mx
        _mlx_load = mlx_load
        _mlx_generate = mlx_generate
        _mlx_core = mx
    return _mlx_load, _mlx_generate, _mlx_core


def get_model():
    """Get or load the MLX embedding model (one-time load)."""
    global _mlx_model, _mlx_tokenizer
    if _mlx_model is not None:
        return _mlx_model, _mlx_tokenizer

    if not _MODEL_CACHE.exists():
        raise RuntimeError(
            f"Embedding model not cached at {_MODEL_CACHE}. "
            f"Run ./setup.sh or ./download-model.sh to download it."
        )

    print(f"Loading {_MODEL_ID} via mlx-embeddings...", file=sys.stderr)
    mlx_load, _, _ = _load_mlx_runtime()
    _mlx_model, _mlx_tokenizer = mlx_load(_MODEL_ID)
    print(f"{_MODEL_ID} ready ({_EMBED_DIM} dims, {_MODEL_CFG['max_tokens']} token context)", file=sys.stderr)
    return _mlx_model, _mlx_tokenizer


def _needs_input_remap() -> bool:
    """Check if the model's __call__ uses 'inputs' instead of 'input_ids'.

    Works around mlx-embeddings gemma3_text models where __call__ expects
    'inputs' but the tokenizer returns 'input_ids'.
    """
    return "gemma" in _MODEL_NAME


def embed_texts(texts: List[str], is_query: bool = False) -> List[List[float]]:
    """Embed texts using the configured model. Adds model-specific prefix."""
    model, tokenizer = get_model()
    _, mlx_generate, mx = _load_mlx_runtime()
    prefix = _SEARCH_PREFIX if is_query else _DOCUMENT_PREFIX
    prefixed = [prefix + t for t in texts]

    if _needs_input_remap():
        # gemma3_text models expect (inputs, attention_mask) not (input_ids, ...)
        encoded = tokenizer.batch_encode_plus(
            prefixed, return_tensors="mlx", padding=True,
            truncation=True, max_length=_MODEL_CFG["max_tokens"],
        )
        output = model(encoded["input_ids"], attention_mask=encoded.get("attention_mask"))
    else:
        output = mlx_generate(model, tokenizer, texts=prefixed,
                              max_length=_MODEL_CFG["max_tokens"])

    embeddings = output.text_embeds.tolist()
    mx.clear_cache()
    return embeddings


# --- Milvus client management ---

_persistent_clients: Dict[str, MilvusClient] = {}
_fts = FTSIndex("turns_fts", [
    "session_id", "git_branch", "turn_index", "timestamp", "chunk_type",
    "project_root", "logical_session_id", "provider", "source_kind",
    "source_class", "source_id", "source_path",
])
_write_lock: Optional[asyncio.Lock] = None
_embed_semaphore: Optional[asyncio.Semaphore] = None
# Dedicated single-worker executor so every MLX/Metal call runs on the same OS
# thread. The asyncio semaphore already serializes calls in time, but the
# default executor can rotate workers between calls — and MLX command-buffer
# state is not safe to migrate across threads. See SESF-8.
_embed_executor: Optional[ThreadPoolExecutor] = None
_server_mode = False


def init_server_mode(db_path: Optional[str] = None):
    """Initialize async concurrency primitives for HTTP server mode."""
    global _write_lock, _embed_semaphore, _embed_executor, _server_mode
    _check_model_identity(db_path=db_path)
    _write_lock = asyncio.Lock()
    _embed_semaphore = asyncio.Semaphore(1)
    _embed_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mlx-embed")
    _server_mode = True
    _fts.set_server_mode(True)
    print(f"Server mode initialized (model: {_MODEL_NAME})", file=sys.stderr)


def close_server_mode():
    """Close all persistent clients (Milvus + FTS) and reset server mode."""
    global _write_lock, _embed_semaphore, _embed_executor, _server_mode
    for path, client in list(_persistent_clients.items()):
        try:
            client.close()
            logger.info("Closed Milvus client: %s", path)
        except Exception as e:
            logger.warning("Error closing Milvus client %s: %s", path, e)
    _persistent_clients.clear()
    _fts.close_all()
    # Nil the semaphore and lock FIRST so any coroutine that wakes up while we
    # are shutting down sees None and takes the CLI fallback path instead of
    # trying to enqueue work onto a torn-down executor.
    _embed_semaphore = None
    _write_lock = None
    if _embed_executor is not None:
        _embed_executor.shutdown(wait=True)
        _embed_executor = None
    _server_mode = False


def _get_persistent_client(db_path: str) -> MilvusClient:
    """Get or create a persistent client for the given DB path.
    On failure, evicts the stale client and retries once."""
    if db_path in _persistent_clients:
        try:
            _persistent_clients[db_path].has_collection(COLLECTION_NAME)
            return _persistent_clients[db_path]
        except Exception as e:
            logger.warning("Stale Milvus client for %s: %s — reconnecting", db_path, e)
            try:
                _persistent_clients[db_path].close()
            except Exception:
                pass
            del _persistent_clients[db_path]

    if not _is_remote_uri(db_path):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        if _is_remote_uri(db_path):
            # Remote Milvus Standalone — default gRPC settings are fine.
            _persistent_clients[db_path] = MilvusClient(db_path)
        else:
            # Milvus Lite — increase gRPC keepalive to 120s to prevent
            # GOAWAY/ENHANCE_YOUR_CALM (Lite rejects default 10s as too_many_pings).
            _persistent_clients[db_path] = MilvusClient(
                db_path,
                grpc_options={
                    "grpc.keepalive_time_ms": 120_000,
                    "grpc.keepalive_timeout_ms": 20_000,
                },
            )
        logger.info("Opened client: %s", db_path)
    except Exception as e:
        logger.error("Failed to connect to Milvus at %s: %s", db_path, e)
        raise
    return _persistent_clients[db_path]


def _resolve_db_path(db_path: Optional[str]) -> str:
    if not db_path:
        raise ValueError("db_path is required. Global index is at ~/.sessionflow/milvus.db")
    return db_path


def _expected_schema_fields() -> List[FieldSchema]:
    """Source-of-truth Milvus field list for the sessions collection.

    Used by both _ensure_collection() (create path) and _detect_schema_drift()
    (startup validation) so the two can't drift out of sync.
    """
    return [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=_EMBED_DIM),
        FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="doc_id", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="session_id", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="logical_session_id", dtype=DataType.VARCHAR, max_length=256),
        FieldSchema(name="provider", dtype=DataType.VARCHAR, max_length=64),
        FieldSchema(name="source_kind", dtype=DataType.VARCHAR, max_length=96),
        FieldSchema(name="source_class", dtype=DataType.VARCHAR, max_length=32),
        FieldSchema(name="source_id", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="source_path", dtype=DataType.VARCHAR, max_length=1024),
        FieldSchema(name="transcript_file", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="turn_index", dtype=DataType.INT64),
        FieldSchema(name="timestamp", dtype=DataType.VARCHAR, max_length=64),
        FieldSchema(name="git_branch", dtype=DataType.VARCHAR, max_length=256),
        FieldSchema(name="chunk_type", dtype=DataType.VARCHAR, max_length=64),
        FieldSchema(name="project_root", dtype=DataType.VARCHAR, max_length=512),
    ]


def detect_schema_drift(client: MilvusClient) -> List[str]:
    """Return a list of missing or extra field names if the live collection
    schema differs from `_expected_schema_fields()`. Empty list = no drift.

    Only field NAMES are diffed today — pymilvus's describe_collection output
    shape varies across Milvus Lite vs Standalone, and we have not hit a
    case where a same-named field changed dtype/length silently.
    """
    if not client.has_collection(COLLECTION_NAME):
        return []
    try:
        info = client.describe_collection(COLLECTION_NAME)
    except Exception as exc:
        print(f"Schema drift check skipped: describe_collection failed: {exc}", file=sys.stderr)
        return []
    expected = {f.name for f in _expected_schema_fields()}
    actual: set[str] = set()
    for field in info.get("fields", []) or []:
        if isinstance(field, dict):
            name = field.get("name")
        else:
            name = getattr(field, "name", None)
        if name:
            actual.add(name)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    return [f"missing:{n}" for n in missing] + [f"extra:{n}" for n in extra]


def migrate_schema(client: MilvusClient, db_path: str = "") -> None:
    """Drop the sessions collection and recreate it with the current schema.

    DESTRUCTIVE: all indexed turns are lost. Provided as the explicit recovery
    path for `python cleanup.py migrate-schema` and for the auto-migrate
    env opt-in (SESSIONFLOW_AUTO_MIGRATE_SCHEMA=1).
    """
    if client.has_collection(COLLECTION_NAME):
        print(
            f"Dropping collection {COLLECTION_NAME!r} for schema migration "
            "(all indexed turns will be lost)",
            file=sys.stderr,
        )
        client.drop_collection(COLLECTION_NAME)
    _create_collection(client, db_path)


def _ensure_collection(client: MilvusClient, db_path: str = "") -> None:
    """Create the sessions collection if missing; refuse to start on schema drift.

    SESF-11: previously this was create-if-missing only, so adding a field to
    `_expected_schema_fields()` silently broke every insert with
    DataNotMatchException against the pre-existing Milvus collection. Now:
      - missing collection → create
      - present + no drift → no-op
      - present + drift → if SESSIONFLOW_AUTO_MIGRATE_SCHEMA=1 drop+recreate,
        else raise RuntimeError telling the operator to run
        `python cleanup.py migrate-schema`.
    """
    if not client.has_collection(COLLECTION_NAME):
        _create_collection(client, db_path)
        return

    drift = detect_schema_drift(client)
    if not drift:
        return

    auto = os.getenv("SESSIONFLOW_AUTO_MIGRATE_SCHEMA", "").lower() in {"1", "true", "yes", "on"}
    if auto:
        print(
            f"SESSIONFLOW_AUTO_MIGRATE_SCHEMA detected schema drift {drift!r}; "
            "dropping and recreating (all turns lost).",
            file=sys.stderr,
        )
        migrate_schema(client, db_path)
        return

    raise RuntimeError(
        f"Milvus collection {COLLECTION_NAME!r} schema is out of date "
        f"(drift={drift}). Run `python cleanup.py migrate-schema` to drop "
        f"and recreate it (destructive), or set "
        f"SESSIONFLOW_AUTO_MIGRATE_SCHEMA=1 to migrate on startup."
    )


def _create_collection(client: MilvusClient, db_path: str = "") -> None:
    print(f"Creating collection: {COLLECTION_NAME} (dim={_EMBED_DIM})", file=sys.stderr)
    schema = CollectionSchema(fields=_expected_schema_fields())

    index_params = client.prepare_index_params()
    if _is_remote_uri(db_path):
        # Standalone supports HNSW — O(log n) search vs O(n) FLAT.
        index_params.add_index(
            field_name="vector",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 256},
        )
    else:
        # Milvus Lite silently ignores non-FLAT indexes.
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="COSINE")

    client.create_collection(
        collection_name=COLLECTION_NAME,
        schema=schema,
        index_params=index_params,
    )

    print(f"Collection created: {COLLECTION_NAME}", file=sys.stderr)

    # Standalone requires explicit load_collection before query/dedup paths work.
    # create_collection with index_params does not auto-load.
    if _is_remote_uri(db_path):
        client.load_collection(collection_name=COLLECTION_NAME)
        print(f"Collection loaded: {COLLECTION_NAME}", file=sys.stderr)


@contextmanager
def milvus_client_for_migration(db_path: Optional[str] = None):
    """Open a Milvus client WITHOUT _ensure_collection.

    SESF-11: needed because _ensure_collection refuses to start on schema
    drift — but the whole point of `cleanup.py migrate-schema` is to repair
    that drift. This bypass MUST NOT be used outside migration code paths.
    """
    path = _resolve_db_path(db_path)
    if not _is_remote_uri(path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    client = MilvusClient(path)
    try:
        yield client
    finally:
        client.close()


@contextmanager
def milvus_client(db_path: Optional[str] = None):
    """Get a Milvus client. In server mode, reuses persistent client."""
    path = _resolve_db_path(db_path)

    if _server_mode:
        client = _get_persistent_client(path)
        _ensure_collection(client, path)
        yield client
    else:
        if not _is_remote_uri(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        client = MilvusClient(path)
        _ensure_collection(client, path)
        try:
            yield client
        finally:
            client.close()


# --- Core operations ---

def add_turns(turns: List[Dict], db_path: Optional[str] = None) -> int:
    """Insert conversation turn chunks into Milvus. Dedup by doc_id.

    Each turn dict should have:
        text, doc_id, session_id, transcript_file, turn_index,
        timestamp, git_branch, chunk_type
    """
    if not turns:
        return 0

    # Dedup: check which doc_ids already exist
    with milvus_client(db_path) as client:
        existing_ids = set()
        for turn in turns:
            doc_id = turn["doc_id"]
            try:
                results = client.query(
                    collection_name=COLLECTION_NAME,
                    filter=f'doc_id == "{doc_id}"',
                    limit=1,
                    output_fields=["doc_id"],
                )
                if results:
                    existing_ids.add(doc_id)
            except Exception as e:
                logger.warning("Dedup check failed for doc_id %s: %s", doc_id, e)

    new_turns = [t for t in turns if t["doc_id"] not in existing_ids]
    if not new_turns:
        return 0

    # Embed texts in local, resource-controlled batches. Query embedding stays
    # untouched in search(); this path is ingestion/backfill only.
    budget = get_embedding_budget()
    all_embeddings = []
    for batch in budget.split_batches(new_turns):
        texts = [t["text"] for t in batch]
        decision = budget.before_batch(
            batch_size=len(batch),
            estimated_chars=sum(len(t) for t in texts),
        )
        if not decision.allowed and decision.retry_after_seconds > 0:
            time.sleep(decision.retry_after_seconds)
            decision = budget.before_batch(
                batch_size=len(batch),
                estimated_chars=sum(len(t) for t in texts),
            )
        if not decision.allowed:
            logger.info("Embedding batch deferred: %s", decision.reason)
            break

        started = time.monotonic()
        try:
            embeddings = embed_texts(texts, is_query=False)
        except Exception as e:
            budget.after_batch(time.monotonic() - started, 0, error=e)
            raise
        budget.after_batch(time.monotonic() - started, len(batch))
        all_embeddings.extend(embeddings)

    new_turns = new_turns[:len(all_embeddings)]
    embeddings = all_embeddings
    if not new_turns:
        return 0

    provider_defaults = default_provider_metadata()
    data = []
    for turn, emb in zip(new_turns, embeddings):
        # Stable hash: SHA-256 truncated to int64. Python's hash() is
        # randomized per process, so the same doc_id would get different
        # primary keys across server restarts.
        int_id = int(hashlib.sha256(turn["doc_id"].encode()).hexdigest()[:15], 16)
        data.append({
            "id": int_id,
            "vector": emb,
            "document": turn["text"][:65535],
            "doc_id": turn["doc_id"],
            "session_id": turn.get("session_id", ""),
            "logical_session_id": turn.get("logical_session_id", turn.get("session_id", "")),
            "provider": turn.get("provider", provider_defaults["provider"]),
            "source_kind": turn.get("source_kind", provider_defaults["source_kind"]),
            "source_class": turn.get("source_class", provider_defaults["source_class"]),
            "source_id": turn.get("source_id", ""),
            "source_path": turn.get("source_path", turn.get("transcript_file", "")),
            "transcript_file": turn.get("transcript_file", ""),
            "turn_index": turn.get("turn_index", 0),
            "timestamp": turn.get("timestamp", ""),
            "git_branch": turn.get("git_branch", ""),
            "chunk_type": turn.get("chunk_type", "turn"),
            "project_root": turn.get("project_root", ""),
        })

    with milvus_client(db_path) as client:
        client.insert(collection_name=COLLECTION_NAME, data=data)

    # Dual-write into FTS5 sidecar
    try:
        if db_path:
            fts_conn = _fts.connection(db_path)
            fts_records = [{
                "doc_id": t["doc_id"],
                "content": t["text"],
                "session_id": t.get("session_id", ""),
                "logical_session_id": t.get("logical_session_id", t.get("session_id", "")),
                "provider": t.get("provider", provider_defaults["provider"]),
                "source_kind": t.get("source_kind", provider_defaults["source_kind"]),
                "source_class": t.get("source_class", provider_defaults["source_class"]),
                "source_id": t.get("source_id", ""),
                "source_path": t.get("source_path", t.get("transcript_file", "")),
                "git_branch": t.get("git_branch", ""),
                "turn_index": t.get("turn_index", 0),
                "timestamp": t.get("timestamp", ""),
                "chunk_type": t.get("chunk_type", "turn"),
                "project_root": t.get("project_root", ""),
            } for t in new_turns]
            _fts.insert(fts_conn, fts_records)
            _fts.close_ephemeral(fts_conn)
    except Exception as e:
        logger.warning("FTS insert failed (non-fatal): %s", e)

    return len(data)


def _escape_filter_scalar(value: str) -> str:
    """Escape a string value for use in a Milvus boolean-expression filter literal.

    Milvus filter literals use double-quoted strings (e.g. field == "value").
    Rules:
      - NUL bytes are never valid in identifiers or scalar values; reject them
        outright so a malformed input cannot truncate the filter expression.
      - Any embedded double-quote character must be doubled ("" is the escape
        sequence inside a Milvus double-quoted string literal).
    """
    if "\x00" in value:
        raise ValueError("Filter scalar value must not contain NUL bytes")
    return value.replace('"', '""')


def search(query: str, n: int = 5, session_id: Optional[str] = None,
           git_branch: Optional[str] = None, project_root: Optional[str] = None,
           recency_boost: bool = False,
           date_from: Optional[str] = None, date_to: Optional[str] = None,
           provider: Optional[str] = None, source_kind: Optional[str] = None,
           db_path: Optional[str] = None) -> List[Dict]:
    """Hybrid search: vector similarity + FTS5 keyword search, merged with RRF.

    Both engines run with an expanded candidate pool (n*3), then RRF merges
    the two ranked lists. Recency boost is applied after merging.

    project_root: when set, restricts results to that project. When None,
    searches across all projects (cross-project search).
    date_from/date_to: ISO 8601 date strings (e.g. '2026-04-02') to restrict
    results to a time range. Timestamps are VARCHAR and sort lexicographically.
    """
    # Validate provider/source_kind before they reach Milvus filter strings.
    # These flow through to a server-side expression as raw quoted values;
    # rejecting unknown inputs early prevents filter-expression injection.
    if provider is not None and provider not in LEGAL_PROVIDERS:
        allowed = ", ".join(sorted(LEGAL_PROVIDERS))
        raise ValueError(
            f"Invalid provider: {provider!r}; expected one of: {allowed}"
        )
    if source_kind is not None and source_kind not in LEGAL_SOURCE_KINDS:
        allowed = ", ".join(sorted(LEGAL_SOURCE_KINDS))
        raise ValueError(
            f"Invalid source_kind: {source_kind!r}; expected one of: {allowed}"
        )

    # Expanded candidate pool for both engines
    fetch_n = n * 3

    # --- Vector search ---
    query_embedding = embed_texts([query], is_query=True)[0]

    filters = []
    if session_id:
        filters.append(f'session_id == "{_escape_filter_scalar(session_id)}"')
    if git_branch:
        filters.append(f'git_branch == "{_escape_filter_scalar(git_branch)}"')
    if project_root:
        filters.append(f'project_root == "{_escape_filter_scalar(project_root)}"')
    if provider:
        filters.append(f'provider == "{provider}"')
    if source_kind:
        filters.append(f'source_kind == "{source_kind}"')
    if date_from:
        filters.append(f'timestamp >= "{date_from}"')
    if date_to:
        filters.append(f'timestamp <= "{date_to}T23:59:59"')
    filter_expr = " && ".join(filters) if filters else None

    search_params = {"metric_type": "COSINE"}
    if _is_remote_uri(db_path or ""):
        search_params["params"] = {"ef": 128}  # HNSW search parameter

    with milvus_client(db_path) as client:
        results = client.search(
            collection_name=COLLECTION_NAME,
            data=[query_embedding],
            limit=fetch_n,
            filter=filter_expr,
            search_params=search_params,
            output_fields=["document", "doc_id", "session_id", "transcript_file",
                           "turn_index", "timestamp", "git_branch", "chunk_type",
                           "project_root", "logical_session_id", "provider",
                           "source_kind", "source_class", "source_id", "source_path"],
        )

    provider_defaults = default_provider_metadata()
    vector_results = []
    if results and results[0]:
        for hit in results[0]:
            entity = hit["entity"]
            vector_results.append({
                "content": entity["document"],
                "doc_id": entity.get("doc_id", ""),
                "session_id": entity.get("session_id", ""),
                "logical_session_id": entity.get("logical_session_id", entity.get("session_id", "")),
                "provider": entity.get("provider", provider_defaults["provider"]),
                "source_kind": entity.get("source_kind", provider_defaults["source_kind"]),
                "source_class": entity.get("source_class", provider_defaults["source_class"]),
                "source_id": entity.get("source_id", ""),
                "source_path": entity.get("source_path", entity.get("transcript_file", "")),
                "transcript_file": entity.get("transcript_file", ""),
                "turn_index": entity.get("turn_index", 0),
                "timestamp": entity.get("timestamp", ""),
                "git_branch": entity.get("git_branch", ""),
                "chunk_type": entity.get("chunk_type", ""),
                "project_root": entity.get("project_root", ""),
                "distance": hit["distance"],
            })

    # --- FTS5 keyword search ---
    fts_filters = {}
    if session_id:
        fts_filters["session_id"] = session_id
    if git_branch:
        fts_filters["git_branch"] = git_branch
    if project_root:
        fts_filters["project_root"] = project_root
    if provider:
        fts_filters["provider"] = provider
    if source_kind:
        fts_filters["source_kind"] = source_kind
    if date_from:
        fts_filters["timestamp_gte"] = (">=", date_from)
    if date_to:
        fts_filters["timestamp_lte"] = ("<=", f"{date_to}T23:59:59")
    fts_results = _fts.search(query, n=fetch_n, filters=fts_filters or None, db_path=db_path)

    # --- Merge with RRF ---
    if fts_results and vector_results:
        # Both engines returned results — merge
        merged = rrf_merge(vector_results, fts_results, n=fetch_n)
    elif fts_results:
        merged = fts_results
    else:
        merged = vector_results

    if not merged:
        return []

    # Clean up internal RRF score before recency boost
    merge_defaults = default_provider_metadata()
    for r in merged:
        r.pop("_rrf_score", None)
        r.setdefault("provider", merge_defaults["provider"])
        r.setdefault("source_kind", merge_defaults["source_kind"])
        r.setdefault("source_class", merge_defaults["source_class"])
        r.setdefault("logical_session_id", r.get("session_id", ""))

    if recency_boost and merged:
        merged = _apply_recency_boost(merged, n)

    # If the FTS table was recently dropped + recreated and backfill hasn't
    # caught up, surface a one-line warning on each row's metadata so callers
    # can render it without us blocking the search.
    try:
        from fts_hybrid import fts_backfill_required
        if fts_backfill_required():
            notice = "keyword index rebuilding, results may be vector-only"
            for r in merged[:n]:
                r["_fts_warning"] = notice
    except Exception:  # pragma: no cover - sentinel check is best-effort
        pass

    return merged[:n]


def _apply_recency_boost(results: List[Dict], n: int) -> List[Dict]:
    """Re-rank results by combining semantic similarity with recency.

    Score = similarity * (1 + recency_weight * recency_factor)
    where recency_factor is 1.0 for the newest result and 0.0 for the oldest.
    """
    recency_weight = 0.3

    # Parse timestamps and sort to find range
    timestamps = []
    for r in results:
        ts = r.get("timestamp", "")
        if ts:
            try:
                # ISO 8601 strings sort lexicographically
                timestamps.append(ts)
            except Exception:
                timestamps.append("")
        else:
            timestamps.append("")

    if not any(timestamps):
        return results

    valid_ts = [t for t in timestamps if t]
    if len(valid_ts) < 2:
        return results

    ts_min = min(valid_ts)
    ts_max = max(valid_ts)

    for i, r in enumerate(results):
        similarity = 1 - r["distance"]  # COSINE distance → similarity
        ts = timestamps[i]
        if ts and ts_min != ts_max:
            # Normalize timestamp to [0, 1] range
            recency = (valid_ts.index(ts) if ts in valid_ts else 0) / max(len(valid_ts) - 1, 1)
            # Simple linear approach: newer timestamps get higher recency
            # Since ISO strings sort ascending, higher position = newer
            all_sorted = sorted(valid_ts)
            try:
                pos = all_sorted.index(ts)
                recency = pos / max(len(all_sorted) - 1, 1)
            except ValueError:
                recency = 0.5
        else:
            recency = 0.5

        r["_score"] = similarity * (1 + recency_weight * recency)

    results.sort(key=lambda r: r.get("_score", 0), reverse=True)

    # Clean up internal score
    for r in results:
        r.pop("_score", None)

    return results


def get_turns(session_id: str, turn_index: int, context: int = 2,
              db_path: Optional[str] = None) -> List[Dict]:
    """Retrieve turns around a specific turn_index within a session.

    turn_index is a byte offset into the transcript file. context is the
    number of neighboring turns (before and after) to include. We fetch all
    turns for the session, sort by turn_index, find the target, and return
    the surrounding window.

    Returns turns sorted by turn_index ascending, with the same field
    mapping as search() (document → content).
    """
    with milvus_client(db_path) as client:
        results = client.query(
            collection_name=COLLECTION_NAME,
            filter=f'session_id == "{_escape_filter_scalar(session_id)}"',
            output_fields=["document", "doc_id", "session_id", "transcript_file",
                           "turn_index", "timestamp", "git_branch", "chunk_type",
                           "logical_session_id", "provider", "source_kind",
                           "source_class", "source_id", "source_path"],
            limit=16384,
        )

    if not results:
        return []

    # Sort all turns by turn_index (byte offset)
    results.sort(key=lambda r: r.get("turn_index", 0))

    # Find the target turn (closest match to requested turn_index)
    target_idx = 0
    min_dist = float("inf")
    for i, row in enumerate(results):
        dist = abs(row.get("turn_index", 0) - turn_index)
        if dist < min_dist:
            min_dist = dist
            target_idx = i

    # Extract window: context turns before and after
    start = max(0, target_idx - context)
    end = min(len(results), target_idx + context + 1)

    turn_defaults = default_provider_metadata()
    formatted = []
    for row in results[start:end]:
        formatted.append({
            "content": row["document"],
            "doc_id": row.get("doc_id", ""),
            "session_id": row.get("session_id", ""),
            "logical_session_id": row.get("logical_session_id", row.get("session_id", "")),
            "provider": row.get("provider", turn_defaults["provider"]),
            "source_kind": row.get("source_kind", turn_defaults["source_kind"]),
            "source_class": row.get("source_class", turn_defaults["source_class"]),
            "source_id": row.get("source_id", ""),
            "source_path": row.get("source_path", row.get("transcript_file", "")),
            "transcript_file": row.get("transcript_file", ""),
            "turn_index": row.get("turn_index", 0),
            "timestamp": row.get("timestamp", ""),
            "git_branch": row.get("git_branch", ""),
            "chunk_type": row.get("chunk_type", ""),
        })

    return formatted


def get_stats(project_root: Optional[str] = None, db_path: Optional[str] = None) -> Dict:
    """Get index statistics. Optionally filter to a specific project."""
    with milvus_client(db_path) as client:
        if not client.has_collection(COLLECTION_NAME):
            return {"total_turns": 0, "sessions": 0, "by_type": {}, "providers": {}}

    # Query for breakdowns (capped by Milvus offset limit)
    all_results = _query_all(
        ["session_id", "chunk_type", "git_branch", "project_root", "provider"],
        filter_expr=f'project_root == "{_escape_filter_scalar(project_root)}"' if project_root else None,
        db_path=db_path,
    )

    total = len(all_results)
    sessions = set(r["session_id"] for r in all_results if r.get("session_id"))
    branches = set(r["git_branch"] for r in all_results if r.get("git_branch"))

    by_type = {}
    providers = {}
    defaults = default_provider_metadata()
    for r in all_results:
        t = r.get("chunk_type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1
        provider_name = r.get("provider", defaults["provider"])
        providers[provider_name] = providers.get(provider_name, 0) + 1

    return {
        "total_turns": total,
        "sessions": len(sessions),
        "branches": sorted(branches),
        "by_type": by_type,
        "providers": providers,
    }


def _query_batches(output_fields: list, batch_size: int = 1000,
                   filter_expr: Optional[str] = None,
                   db_path: Optional[str] = None) -> Iterator[list]:
    """Yield Milvus query_iterator results one batch at a time.

    Uses pymilvus's server-side iterator instead of offset pagination so the
    full collection is drained regardless of size. The previous implementation
    hard-capped at 16,384 rows, silently truncating any collection larger than
    that — see SESF-4.
    """
    with milvus_client(db_path) as client:
        if not client.has_collection(COLLECTION_NAME):
            return
        iterator = client.query_iterator(
            collection_name=COLLECTION_NAME,
            batch_size=batch_size,
            filter=filter_expr or "",
            output_fields=output_fields,
        )
        try:
            while True:
                batch = iterator.next()
                if not batch:
                    break
                yield batch
        finally:
            iterator.close()


def _query_all(output_fields: list, batch_size: int = 1000,
               filter_expr: Optional[str] = None,
               db_path: Optional[str] = None) -> list:
    """Query all rows via Milvus query_iterator. Optional filter expression.

    Keeps the public list-returning behavior for callers that need aggregate
    results while allowing streaming callers to use _query_batches directly.
    """
    all_results = []
    for batch in _query_batches(output_fields, batch_size, filter_expr, db_path):
        all_results.extend(batch)
    return all_results


# --- Cleanup operations ---

def delete_by_session(session_id: str, db_path: Optional[str] = None) -> int:
    """Delete all turns for a given session ID."""
    escaped_sid = _escape_filter_scalar(session_id)
    with milvus_client(db_path) as client:
        results = client.query(
            collection_name=COLLECTION_NAME,
            filter=f'session_id == "{escaped_sid}"',
            output_fields=["id"],
        )
        if results:
            client.delete(
                collection_name=COLLECTION_NAME,
                filter=f'session_id == "{escaped_sid}"',
            )

    # Also delete from FTS
    try:
        if db_path:
            conn = _fts.connection(db_path)
            _fts.delete(conn, "session_id", session_id)
            _fts.close_ephemeral(conn)
    except Exception as e:
        logger.warning("FTS delete by session failed (non-fatal): %s", e)

    return len(results)


def delete_by_branch(git_branch: str, db_path: Optional[str] = None) -> int:
    """Delete all turns for a given git branch."""
    escaped_branch = _escape_filter_scalar(git_branch)
    with milvus_client(db_path) as client:
        results = client.query(
            collection_name=COLLECTION_NAME,
            filter=f'git_branch == "{escaped_branch}"',
            output_fields=["id"],
        )
        if results:
            client.delete(
                collection_name=COLLECTION_NAME,
                filter=f'git_branch == "{escaped_branch}"',
            )

    # Also delete from FTS
    try:
        if db_path:
            conn = _fts.connection(db_path)
            _fts.delete(conn, "git_branch", git_branch)
            _fts.close_ephemeral(conn)
    except Exception as e:
        logger.warning("FTS delete by branch failed (non-fatal): %s", e)

    return len(results)


def delete_older_than(max_age_days: int, db_path: Optional[str] = None) -> int:
    """Delete all turns with timestamps older than max_age_days ago.

    Returns the number of deleted turns.
    """
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S")

    # Milvus Lite varchar comparison works lexicographically,
    # and ISO 8601 timestamps sort correctly this way.
    with milvus_client(db_path) as client:
        if not client.has_collection(COLLECTION_NAME):
            return 0

        # Query to count before deleting
        results = client.query(
            collection_name=COLLECTION_NAME,
            filter=f'timestamp < "{cutoff_str}" && timestamp != ""',
            output_fields=["id"],
            limit=16384,
        )
        if results:
            client.delete(
                collection_name=COLLECTION_NAME,
                filter=f'timestamp < "{cutoff_str}" && timestamp != ""',
            )

    # Also delete from FTS
    try:
        if db_path:
            conn = _fts.connection(db_path)
            _fts.delete_where(conn, "timestamp < ? AND timestamp != ''", (cutoff_str,))
            _fts.close_ephemeral(conn)
    except Exception as e:
        logger.warning("FTS delete older_than failed (non-fatal): %s", e)

    return len(results)


def backfill_fts(db_path: Optional[str] = None) -> int:
    """Populate FTS from Milvus for any records missing from the FTS index.

    Two-pass to stay under Milvus's 64MB per-segment query result limit:
      1. Fetch doc_id only (~100 bytes/row) to identify what's in Milvus.
      2. Diff against FTS via batched IN-clause to find missing doc_ids.
      3. Fetch full documents only for missing doc_ids, in chunks of 100.
    The original single-pass that pulled the wide `document` field on all
    rows crossed the 64MB ceiling once a segment accumulated enough text
    and corrupted Woodpecker WAL state. See SESF-2.
    """
    if not db_path:
        return 0

    fts_conn = _fts.connection(db_path)
    try:
        # Pass 2: hydrate missing rows in small batches and stream into FTS
        # one batch at a time so peak memory stays at O(BATCH_FETCH) regardless
        # of how many rows are missing — see SESF-5.
        output_fields = ["doc_id", "document", "session_id", "git_branch",
                         "turn_index", "timestamp", "chunk_type", "project_root",
                         "logical_session_id", "provider", "source_kind",
                         "source_class", "source_id", "source_path"]
        backfill_defaults = default_provider_metadata()
        BATCH_FETCH = 100
        inserted = 0

        with milvus_client(db_path) as client:
            def hydrate_and_insert(doc_ids: list) -> None:
                nonlocal inserted
                for i in range(0, len(doc_ids), BATCH_FETCH):
                    fetch_chunk = doc_ids[i:i + BATCH_FETCH]
                    ids_quoted = ", ".join(json.dumps(d) for d in fetch_chunk)
                    batch = client.query(
                        collection_name=COLLECTION_NAME,
                        filter=f"doc_id in [{ids_quoted}]",
                        limit=len(fetch_chunk),
                        output_fields=output_fields,
                    )
                    records = [
                        {
                            "doc_id": r["doc_id"],
                            "content": r.get("document", ""),
                            "session_id": r.get("session_id", ""),
                            "logical_session_id": r.get("logical_session_id", r.get("session_id", "")),
                            "provider": r.get("provider", backfill_defaults["provider"]),
                            "source_kind": r.get("source_kind", backfill_defaults["source_kind"]),
                            "source_class": r.get("source_class", backfill_defaults["source_class"]),
                            "source_id": r.get("source_id", ""),
                            "source_path": r.get("source_path", r.get("transcript_file", "")),
                            "git_branch": r.get("git_branch", ""),
                            "turn_index": r.get("turn_index", 0),
                            "timestamp": r.get("timestamp", ""),
                            "chunk_type": r.get("chunk_type", "turn"),
                            "project_root": r.get("project_root", ""),
                        }
                        for r in batch
                    ]
                    if records:
                        _fts.insert(fts_conn, records)
                        inserted += len(records)

            # Diff against FTS in bounded chunks, then hydrate each chunk before
            # moving on so missing doc IDs never grow with collection size.
            for batch in _query_batches(["doc_id"], batch_size=500, db_path=db_path):
                chunk = [r.get("doc_id", "") for r in batch if r.get("doc_id", "")]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _ in chunk)
                rows = fts_conn.execute(
                    f"SELECT doc_id FROM {_fts.table_name} WHERE doc_id IN ({placeholders})",
                    chunk,
                ).fetchall()
                existing = {row[0] for row in rows}
                missing_doc_ids = [d for d in chunk if d not in existing]
                if missing_doc_ids:
                    hydrate_and_insert(missing_doc_ids)

        logger.info("FTS backfill: inserted %d records", inserted)
        # Sentinel-driven warning is no longer relevant once we've repopulated.
        try:
            from fts_hybrid import clear_fts_backfill_sentinel
            clear_fts_backfill_sentinel()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to clear FTS sentinel: %s", exc)
        return inserted
    finally:
        _fts.close_ephemeral(fts_conn)


def clear_collection(db_path: Optional[str] = None):
    """Drop and recreate the collection (full reset). Also clears FTS."""
    with milvus_client(db_path) as client:
        if client.has_collection(COLLECTION_NAME):
            client.drop_collection(COLLECTION_NAME)
            print(f"Collection dropped: {COLLECTION_NAME}", file=sys.stderr)

    # Clear FTS database
    if db_path:
        _fts.clear(db_path)


def list_sessions(project_root: Optional[str] = None,
                  db_path: Optional[str] = None) -> List[Dict]:
    """List all sessions with turn counts and date ranges. Optionally filter by project."""
    all_results = _query_all(
        ["session_id", "timestamp", "git_branch", "chunk_type", "project_root"],
        filter_expr=f'project_root == "{_escape_filter_scalar(project_root)}"' if project_root else None,
        db_path=db_path,
    )

    sessions: Dict[str, Dict] = {}
    for r in all_results:
        sid = r.get("session_id", "")
        if not sid:
            continue
        if sid not in sessions:
            sessions[sid] = {
                "session_id": sid,
                "turns": 0,
                "branches": set(),
                "min_ts": "",
                "max_ts": "",
            }
        s = sessions[sid]
        s["turns"] += 1
        branch = r.get("git_branch", "")
        if branch:
            s["branches"].add(branch)
        ts = r.get("timestamp", "")
        if ts:
            if not s["min_ts"] or ts < s["min_ts"]:
                s["min_ts"] = ts
            if not s["max_ts"] or ts > s["max_ts"]:
                s["max_ts"] = ts

    result = []
    for s in sessions.values():
        s["branches"] = sorted(s["branches"])
        result.append(s)

    # Sort by most recent first
    result.sort(key=lambda s: s["max_ts"], reverse=True)
    return result


# --- Async wrappers ---

async def search_async(query: str, n: int = 5, session_id: Optional[str] = None,
                       git_branch: Optional[str] = None, project_root: Optional[str] = None,
                       recency_boost: bool = False,
                       provider: Optional[str] = None, source_kind: Optional[str] = None,
                       db_path: Optional[str] = None) -> List[Dict]:
    """Async search with embed semaphore."""
    loop = asyncio.get_event_loop()
    # Snapshot globals at function entry (before any await) to close the TOCTOU
    # window: close_server_mode can clear _embed_executor between the semaphore
    # guard and the run_in_executor call.
    executor = _embed_executor
    semaphore = _embed_semaphore

    if semaphore is None or executor is None:
        raise RuntimeError(
            "MLX embed executor not initialized — call init_server_mode() before "
            "using search_async(). Running embeddings on the default executor can "
            "hop OS threads and trigger Metal SIGSEGV (see SESF-8)."
        )
    async with semaphore:
        return await loop.run_in_executor(
            executor,
            lambda: search(
                query, n, session_id=session_id, git_branch=git_branch,
                project_root=project_root, recency_boost=recency_boost,
                provider=provider, source_kind=source_kind, db_path=db_path,
            ),
        )


async def add_turns_async(turns: List[Dict], db_path: Optional[str] = None) -> int:
    """Async add_turns with embed semaphore + write lock."""
    loop = asyncio.get_event_loop()
    # Snapshot globals at function entry (before any await) to close the TOCTOU
    # window: close_server_mode can clear _embed_executor between the semaphore
    # guard and the run_in_executor call.
    executor = _embed_executor
    semaphore = _embed_semaphore
    write_lock = _write_lock

    if semaphore is None or write_lock is None or executor is None:
        raise RuntimeError(
            "MLX embed executor not initialized — call init_server_mode() before "
            "using add_turns_async(). Running embeddings on the default executor can "
            "hop OS threads and trigger Metal SIGSEGV (see SESF-8)."
        )
    async with semaphore:
        async with write_lock:
            return await loop.run_in_executor(executor, lambda: add_turns(turns, db_path))
