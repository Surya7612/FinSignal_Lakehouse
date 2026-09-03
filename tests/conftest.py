"""Shared pytest fixtures."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.utils.io import create_spark_session

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def project_root() -> Path:
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def java_home_configured() -> None:
    if not os.environ.get("JAVA_HOME"):
        candidates = [
            "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
            "/usr/lib/jvm/java-17-openjdk-amd64",
            "/usr/lib/jvm/java-17-amazon-corretto",
        ]
        for candidate in candidates:
            if Path(candidate).exists():
                os.environ["JAVA_HOME"] = candidate
                os.environ["PATH"] = f"{candidate}/bin:{os.environ.get('PATH', '')}"
                break


@pytest.fixture(scope="module")
def spark(java_home_configured):
    session = create_spark_session("FinSignal Integration Tests")
    yield session
    session.stop()
