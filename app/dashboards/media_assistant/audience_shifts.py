import streamlit as st
import pandas as pd

def render(df: pd.DataFrame):
    st.subheader("👥 Audience Shifts")
    st.info("Add audience dimension queries here (age/geo/device) and visuals.")
