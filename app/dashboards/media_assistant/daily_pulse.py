import streamlit as st
import pandas as pd
from app.ui.components.components import pretty_rates

def render(df: pd.DataFrame):
    st.subheader("📅 Daily Pulse")
    if df.empty: st.info("No data."); return
    d = df.copy(); d["date"] = pd.to_datetime(d["date"])

    last = d["date"].max()
    w2 = (last - pd.Timedelta(days=6), last)
    w1 = (w2[0] - pd.Timedelta(days=7), w2[0] - pd.Timedelta(days=1))

    def agg(window):
        a, b = window
        dd = d[(d["date"]>=a)&(d["date"]<=b)]
        g = dd.groupby("platform")[["impressions","clicks","cost","conversions"]].sum()
        return g.rename(columns=lambda c: f"{c}_{'w2' if window==w2 else 'w1'}")

    m = agg(w1).join(agg(w2), how="outer").fillna(0)
    m["ctr_w1"] = m["clicks_w1"]/m["impressions_w1"].replace(0, pd.NA)
    m["ctr_w2"] = m["clicks_w2"]/m["impressions_w2"].replace(0, pd.NA)
    m["cpc_w1"] = m["cost_w1"]/m["clicks_w1"].replace(0, pd.NA)
    m["cpc_w2"] = m["cost_w2"]/m["clicks_w2"].replace(0, pd.NA)
    m["cvr_w1"] = m["conversions_w1"]/m["clicks_w1"].replace(0, pd.NA)
    m["cvr_w2"] = m["conversions_w2"]/m["clicks_w2"].replace(0, pd.NA)
    m["ctr_wow"] = (m["ctr_w2"]-m["ctr_w1"])/m["ctr_w1"].replace(0, pd.NA)
    m["cpc_wow"] = (m["cpc_w2"]-m["cpc_w1"])/m["cpc_w1"].replace(0, pd.NA)
    m["cvr_wow"] = (m["cvr_w2"]-m["cvr_w1"])/m["cvr_w1"].replace(0, pd.NA)

    view = m.reset_index()[["platform","ctr_w1","ctr_w2","ctr_wow","cpc_w1","cpc_w2","cpc_wow","cvr_w1","cvr_w2","cvr_wow"]]
    pretty_rates(view, {
        "ctr_w1":"{:.2%}","ctr_w2":"{:.2%}","ctr_wow":"{:+.1%}",
        "cpc_w1":"${:,.2f}","cpc_w2":"${:,.2f}","cpc_wow":"{:+.1%}",
        "cvr_w1":"{:.2%}","cvr_w2":"{:.2%}","cvr_wow":"{:+.1%}",
    })

    st.markdown("**Daily clicks by platform**")
    day = d.groupby(["date","platform"], as_index=False)["clicks"].sum().sort_values("date")
    st.line_chart(day.pivot(index="date", columns="platform", values="clicks"))
