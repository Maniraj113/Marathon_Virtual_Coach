"""Athlete Analyzer - Package Entry Point for ADK Web."""

import sys
from pathlib import Path

# Ensure absolute imports work when running via `adk web .`
# Since this is now in the `agents` folder, the root project directory is one level up
_pkg_dir = str(Path(__file__).resolve().parent.parent)
if _pkg_dir not in sys.path:
    sys.path.insert(0, _pkg_dir)

from agents.coach_agent import coaching_engine

# ADK web discovers the agent via `root_agent`.
# This MUST be a BaseAgent subclass instance — pointing to the real pipeline,
# not a shell LlmAgent, so all 5 pipeline stages are executed for every message.
root_agent = coaching_engine.pipeline

__all__ = ["root_agent"]
