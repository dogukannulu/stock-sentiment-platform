"""
Restore the real anthropic module for evaluation tests.
The parent conftest.py mocks anthropic for unit tests,
but evaluation tests need the live Claude API.
"""
import sys
import importlib

# Remove the MagicMock and replace with the real module
sys.modules.pop("anthropic", None)
import anthropic  # noqa: E402 — real module now loaded
sys.modules["anthropic"] = anthropic

# Also reload sentiment_job so it picks up the real anthropic client
sys.modules.pop("flink_jobs.sentiment_job", None)
