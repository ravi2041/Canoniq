import streamlit as st
import pandas as pd
import numpy as np
from datetime import date

def _safe_div(a,b): return np.nan if (b in (0,None) or pd.isna(b)) else a/b
def _elapsed_frac(s,e,t):
    if s>e: return 0.0
    tot=(e-s).days+1
    if tot<=0: return 0.0
    if t<s: return 0.0
    if t>e: return 1.0
    return ((t-s).days+1)/tot

def render(df: pd.DataFrame):
    st.subheader("💸 Budget Pacing")
    if df.empty: st.info("No data."); return
    d=df.copy(); d["date"]=pd.to_datetime(d["date"]).dt.date
    if "budgets" not in st.session_state: st.session_state["budgets"]=[]

    with st.form("budget_form"):
        level = st.selectbox("Budget level", ["platform","campaign","creative","placement"])
        options = sorted(d[level].astype(str).dropna().unique())
        keys = st.multiselect(f"Select {level}(s)", options)
        col1,col2 = st.columns(2)
        with col1:
            amt = st.number_input("Budget (total)", min_value=0.0, step=100.0)
        with col2:
            currency = st.text_input("Currency", value="NZD")
        min_d, max_d = d["date"].min(), d["date"].max()
        start,end = st.date_input("Budget period", value=(min_d, max_d), min_value=min_d, max_value=max_d)
        if st.form_submit_button("Add budget") and keys and amt>0:
            st.session_state["budgets"].append({"level":level,"keys":list(map(str,keys)),
                                                "budget_total":float(amt),"currency":currency,
                                                "start":start,"end":end})
            st.success("Budget added.")

    if st.session_state["budgets"]:
        st.markdown("**Budgets**")
        st.dataframe(pd.DataFrame(st.session_state["budgets"]), use_container_width=True)

        today = d["date"].max()
        rows=[]
        for b in st.session_state["budgets"]:
            level=b["level"]; keys=set(b["keys"])
            w = d[(d["date"]>=b["start"]) & (d["date"]<=b["end"]) & (d[level].astype(str).isin(keys))]
            actual = float(w["cost"].sum())
            target = b["budget_total"] * _elapsed_frac(b["start"], b["end"], today)
            pacing = _safe_div(actual, target)
            rows.append({
                "level":level,"keys":", ".join(keys),
                "period":f"{b['start']} → {b['end']}",
                "budget_total":b["budget_total"],
                "target_to_date":target,
                "actual_spend":actual,
                "variance":actual-target,
                "pacing_pct":pacing,
                "currency":b["currency"],
            })
        score=pd.DataFrame(rows)
        st.dataframe(score.style.format({
            "budget_total":"${:,.2f}",
            "target_to_date":"${:,.2f}",
            "actual_spend":"${:,.2f}",
            "variance":"${:,.2f}",
            "pacing_pct":"{:.1%}",
        }), use_container_width=True)
    else:
        st.info("Add a budget to see pacing.")
