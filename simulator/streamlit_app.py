"""Standalone entry point for the dashboard-integrated simulator view."""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st

from dashboard.components.simulator_view import render_simulator_view

st.set_page_config(page_title="ModelPulse Drift Simulator", page_icon="⚡", layout="wide")
render_simulator_view(os.getenv("MODELPULSE_API_URL", "http://localhost:8000"))
