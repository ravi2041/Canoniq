import streamlit as st
from . import daily_pulse, creative_winners, budget_pacing, audience_shifts

def render(df):
    tabs = st.tabs(["Daily Pulse","Creative Winners/Losers","Budget Pacing","Audience Shifts"])
    with tabs[0]: daily_pulse.render(df)
    with tabs[1]: creative_winners.render(df)
    with tabs[2]: budget_pacing.render(df)
    with tabs[3]: audience_shifts.render(df)
