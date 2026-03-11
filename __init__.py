"""Athlete Analyzer - Package Entry Point for ADK Web."""

import sys
from pathlib import Path

# Ensure absolute imports work when running via `adk web .`
_pkg_dir = str(Path(__file__).resolve().parent)
if _pkg_dir not in sys.path:
    sys.path.insert(0, _pkg_dir)

from coach_agent import coaching_engine

# ADK web discovers the agent via `root_agent`.
# This MUST be a BaseAgent subclass instance — pointing to the real pipeline,
# not a shell LlmAgent, so all 5 pipeline stages are executed for every message.
root_agent = coaching_engine.pipeline

__all__ = ["root_agent"]
