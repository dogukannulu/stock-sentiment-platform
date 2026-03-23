"""
Mock pyflink and anthropic at import time so sentiment_job.py
can be imported in tests without requiring the full Flink runtime.
"""
import sys
from unittest.mock import MagicMock


class _MapFunction:
    """Minimal real base class so subclasses remain instantiatable."""
    def open(self, runtime_context):
        pass

    def map(self, value):
        return value


# Build pyflink mock modules
_pyflink_functions = MagicMock()
_pyflink_functions.MapFunction = _MapFunction

for mod, mock in [
    ("pyflink",                                MagicMock()),
    ("pyflink.datastream",                     MagicMock()),
    ("pyflink.datastream.connectors",          MagicMock()),
    ("pyflink.datastream.connectors.kafka",    MagicMock()),
    ("pyflink.datastream.functions",           _pyflink_functions),
    ("pyflink.common",                         MagicMock()),
    ("pyflink.common.serialization",           MagicMock()),
    ("anthropic",                              MagicMock()),
]:
    sys.modules[mod] = mock
