"""SESF-13: FTS5 SQLite connections are thread-bound.

Server-mode persistent caching MUST be per-thread, and close_all() called
from a non-owning thread MUST NOT raise.
"""

import logging
import threading

import pytest


def _index(tmp_path):
    from fts_hybrid import FTSIndex

    fts = FTSIndex("turns_fts", ["session_id", "git_branch", "turn_index", "timestamp", "chunk_type"])
    fts.set_server_mode(True)
    return fts, str(tmp_path / "milvus.db")


def test_server_mode_connections_are_per_thread(tmp_path):
    fts, db_path = _index(tmp_path)
    handles: dict[str, int] = {}

    def worker(label: str):
        conn = fts.connection(db_path)
        handles[label] = id(conn)

    t1 = threading.Thread(target=worker, args=("a",))
    t1.start(); t1.join()
    t2 = threading.Thread(target=worker, args=("b",))
    t2.start(); t2.join()

    assert handles["a"] != handles["b"], "expected distinct per-thread connections"


def test_close_all_swallows_cross_thread_violation(tmp_path, caplog):
    """A connection opened on thread A is left for thread A; close_all on
    thread B does not raise and does not log a WARNING for the violation."""
    fts, db_path = _index(tmp_path)

    opened = threading.Event()
    done = threading.Event()

    def opener():
        fts.connection(db_path)
        opened.set()
        done.wait(timeout=5)

    th = threading.Thread(target=opener)
    th.start()
    assert opened.wait(timeout=5)

    with caplog.at_level(logging.WARNING, logger="fts-hybrid"):
        fts.close_all()  # main thread — different from `opener`

    warns = [r for r in caplog.records if "Error closing" in r.getMessage()]
    assert warns == [], f"unexpected WARN logs: {[r.getMessage() for r in warns]}"

    done.set()
    th.join()


def test_same_thread_open_close_remains_clean(tmp_path, caplog):
    fts, db_path = _index(tmp_path)
    fts.connection(db_path)

    with caplog.at_level(logging.WARNING, logger="fts-hybrid"):
        fts.close_all()

    warns = [r for r in caplog.records if "Error closing" in r.getMessage()]
    assert warns == []
