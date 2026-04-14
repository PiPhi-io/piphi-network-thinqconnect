from __future__ import annotations

import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
TESTKIT_SRC_PATH = PROJECT_ROOT.parent / "piphi-runtime-testkit-python" / "src"
RUNTIME_KIT_SRC_PATH = PROJECT_ROOT.parent / "piphi-runtime-kit-python" / "src"

for path in (SRC_PATH, TESTKIT_SRC_PATH, RUNTIME_KIT_SRC_PATH):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from piphi_runtime_testkit_python import MockCoreServer  # noqa: E402


@pytest.fixture
def mock_core():
    server = MockCoreServer()
    try:
        yield server
    finally:
        server.shutdown()
