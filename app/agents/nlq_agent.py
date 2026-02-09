# app/agents/nlq_agent.py
# Wrap your existing agent so the rest of the app imports from one place.

# --- bootstrap project root on sys.path ---
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# If your agent lives at project root as nlq_nlp_agent_langgraph.py:
from nlq_nlp_agent_langgraph import run_langgraph_agent # noqa: F401

# If it’s elsewhere, adjust the import path accordingly:
# from some_path.nlq_nlp_agent_langgraph import run_langgraph_agent