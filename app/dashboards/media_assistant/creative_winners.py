import streamlit as st
import pandas as pd

def render(df: pd.DataFrame):
    st.subheader("🎨 Creative Winners / Losers")
    if df.empty: st.info("No data."); return
    g = (df.groupby(["platform","creative"], as_index=False)
           .agg(impressions=("impressions","sum"), clicks=("clicks","sum"),
                cost=("cost","sum"), conversions=("conversions","sum")))
    g["ctr"] = g["clicks"]/g["impressions"].replace(0, pd.NA)
    g["cpc"] = g["cost"]/g["clicks"].replace(0, pd.NA)
    g["cvr"] = g["conversions"]/g["clicks"].replace(0, pd.NA)
    st.markdown("**Top 10 by CTR**")
    st.dataframe(g.sort_values("ctr", ascending=False).head(10), use_container_width=True)
    st.markdown("**Bottom 10 by CTR (imp > 1k)**")
    st.dataframe(g[g["impressions"]>1000].sort_values("ctr", ascending=True).head(10), use_container_width=True)
