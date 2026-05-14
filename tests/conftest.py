"""Shared fixtures for SessionFlow tests."""

import json
import os
import sys
import time
from pathlib import Path

import pytest

# Add project root to sys.path so `import http_server` works.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def tmp_server_dir(tmp_path):
    """
    Create a temporary `.sessionflow` directory under the provided `tmp_path`.
    
    Returns:
        pathlib.Path: Path to the created `.sessionflow` directory.
    """
    server_dir = tmp_path / ".sessionflow"
    server_dir.mkdir()
    return server_dir


@pytest.fixture
def mock_heartbeat_file(tmp_server_dir):
    """
    Create a heartbeat JSON file with the current timestamp, current process PID, and activity "idle" inside the provided server directory.
    
    Parameters:
        tmp_server_dir (pathlib.Path): Directory in which to create the `heartbeat` file.
    
    Returns:
        pathlib.Path: Path to the created `heartbeat` file.
    """
    hb_path = tmp_server_dir / "heartbeat"
    data = {
        "timestamp": time.time(),
        "pid": os.getpid(),
        "activity": "idle",
    }
    hb_path.write_text(json.dumps(data))
    return hb_path


@pytest.fixture
def stale_heartbeat_file(tmp_server_dir):
    """
    Create a heartbeat file with its timestamp set 300 seconds in the past.
    
    Parameters:
        tmp_server_dir (pathlib.Path): Directory in which to create the `heartbeat` file.
    
    Returns:
        pathlib.Path: Path to the created `heartbeat` file containing JSON with keys `timestamp` (float), `pid` (int), and `activity` (str).
    """
    hb_path = tmp_server_dir / "heartbeat"
    data = {
        "timestamp": time.time() - 300,
        "pid": os.getpid(),
        "activity": "idle",
    }
    hb_path.write_text(json.dumps(data))
    return hb_path


@pytest.fixture
def mock_pid_file(tmp_server_dir):
    """
    Create a server PID file named `server.pid` inside the given directory containing the current process PID.
    
    Parameters:
        tmp_server_dir (pathlib.Path): Directory in which to create the `server.pid` file.
    
    Returns:
        pathlib.Path: Path to the created `server.pid` file.
    """
    pid_path = tmp_server_dir / "server.pid"
    pid_path.write_text(str(os.getpid()))
    return pid_path


@pytest.fixture
def script_path():
    """
    Compute the absolute filesystem path to the `sessionflow-server.sh` script located at the project root.
    
    Returns:
        script_path (str): Absolute path to `sessionflow-server.sh`.
    """
    return str(Path(__file__).resolve().parent.parent / "sessionflow-server.sh")
