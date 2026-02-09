import streamlit as st
import pandas as pd
import numpy as np

def safe_rate(num, den):
    return np.nan if (den is None or den == 0 or pd.isna(den)) else num/den

def pretty_rates(df, cols_format):
    return st.dataframe(df.style.format(cols_format), use_container_width=True)
