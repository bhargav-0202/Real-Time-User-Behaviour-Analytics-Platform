import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="User Behavior Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background: #0A0E1A; }
section[data-testid="stSidebar"] { background: #0F1420 !important; border-right: 1px solid #1E2535; }
.kpi-card {
    background: linear-gradient(135deg, #111827 0%, #1a2235 100%);
    border: 1px solid #1E2D45; border-radius: 16px;
    padding: 24px 20px 20px; text-align: center;
    position: relative; overflow: hidden; margin-bottom: 8px;
}
.kpi-label {
    color: #6B7280; font-size: 11px; font-weight: 600;
    letter-spacing: 1.2px; text-transform: uppercase; margin-bottom: 10px;
}
.kpi-value { font-size: 32px; font-weight: 700; letter-spacing: -1px; line-height: 1; }
.kpi-delta { font-size: 12px; font-weight: 500; margin-top: 8px; }
.section-header {
    color: #9CA3AF; font-size: 11px; font-weight: 600;
    letter-spacing: 1.5px; text-transform: uppercase;
    margin: 28px 0 16px; padding-bottom: 8px; border-bottom: 1px solid #1E2535;
}
.badge-live {
    display: inline-block; background: #064E3B; color: #10B981;
    font-size: 10px; font-weight: 600; letter-spacing: 1px;
    padding: 3px 10px; border-radius: 20px; border: 1px solid #10B981;
    text-transform: uppercase;
}
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
.stDeployButton { display: none; }
hr { border-color: #1E2535 !important; }
.stButton button {
    background: #1E2D45 !important; color: #60A5FA !important;
    border: 1px solid #2D4A6E !important; border-radius: 8px !important;
    font-size: 12px !important;
}
</style>
""", unsafe_allow_html=True)


# ── Layout helper — no **spread, no conflicts ─────────────────
def L(title="", y2=False, hleg=False):
    d = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color="#9CA3AF", size=12),
        margin=dict(l=10, r=10, t=44, b=10),
        xaxis=dict(gridcolor="#1E2535", linecolor="#1E2535",
                   tickfont=dict(color="#6B7280", size=11)),
        yaxis=dict(gridcolor="#1E2535", linecolor="#1E2535",
                   tickfont=dict(color="#6B7280", size=11)),
        hoverlabel=dict(bgcolor="#1E2535", font_color="#F9FAFB",
                        bordercolor="#3B82F6"),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color="#9CA3AF", size=11),
            orientation="h" if hleg else "v",
            y=1.12 if hleg else 1,
        ),
    )
    if title:
        d["title"] = dict(text=title, font=dict(color="#E5E7EB", size=14))
    if y2:
        d["yaxis2"] = dict(
            overlaying="y", side="right", showgrid=False,
            tickfont=dict(color="#10B981", size=11),
        )
    return d


# ── Snowflake helpers ──────────────────────────────────────────
def _live():
    if os.getenv("DASHBOARD_DEMO", "").lower() in ("1", "true", "yes"):
        return False
    return all(os.getenv(k) for k in
               ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"))


@st.cache_resource
def _sf():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "MARTS"),
    )


@st.cache_data(ttl=60)
def qry(sql):
    cur = _sf().cursor()
    try:
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        df.columns = [c.upper() for c in df.columns]
        return df
    finally:
        cur.close()


def sin(vals):
    return "('__x__')" if not vals else \
        "(" + ",".join(f"'{v}'" for v in vals) + ")"


# ── Demo data ──────────────────────────────────────────────────
@st.cache_data(ttl=300)
def _demo():
    rng = np.random.default_rng(42)
    cats = ["Electronics","Apparel","Home","Sports","Books",
            "Beauty","Toys","Garden","Auto","Food"]
    ctys = ["US","UK","CA","AU","DE","FR","JP","BR"]
    devs = ["mobile","desktop","tablet"]
    rows = []
    end = pd.Timestamp.utcnow().floor("h")
    for h in range(24 * 30):
        ts = end - pd.Timedelta(hours=h)
        for _ in range(8):
            clicks = int(rng.integers(20, 900))
            orders = int(rng.integers(0, max(2, clicks // 35)))
            rows.append({
                "hour_timestamp":    ts,
                "country":           rng.choice(ctys),
                "device":            rng.choice(devs),
                "product_category":  rng.choice(cats),
                "total_clicks":      clicks,
                "unique_users":      max(1, int(clicks * rng.uniform(0.3, 0.8))),
                "unique_sessions":   max(1, int(clicks * rng.uniform(0.2, 0.6))),
                "total_orders":      orders,
                "total_revenue_usd": float(orders * rng.uniform(18, 250)),
                "conversion_rate_pct": min(
                    float(orders * 100 / max(1, clicks // 35)), 100),
            })
    return pd.DataFrame(rows)


def _filt(df, days, ctys, devs):
    ts = pd.to_datetime(df["hour_timestamp"], utc=True).dt.tz_localize(None)
    cut = pd.Timestamp.now() - pd.Timedelta(days=days)
    m = ((ts >= cut)
         & df["country"].isin(ctys or ["__x__"])
         & df["device"].isin(devs or ["__x__"]))
    out = df.loc[m].copy()
    out["hour_timestamp"] = ts[m]
    return out


def fmt(n):
    try: n = float(n)
    except: return "0"
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return f"{n:,.0f}"


# ── Sidebar ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding:16px 0 8px'>
      <span style='color:#F9FAFB;font-size:18px;font-weight:700'>⚡ Analytics</span><br>
      <span style='color:#6B7280;font-size:11px'>Real-Time Platform</span>
    </div>""", unsafe_allow_html=True)
    st.markdown("---")

    days_back      = st.slider("Days of data", 1, 30, 7)
    country_filter = st.multiselect("Countries",
        ["US","UK","CA","AU","DE","FR","JP","BR"], default=["US","UK","CA"])
    device_filter  = st.multiselect("Devices",
        ["mobile","desktop","tablet"], default=["mobile","desktop","tablet"])
    cat_filter     = st.multiselect("Categories",
        ["Electronics","Apparel","Home","Sports","Books",
         "Beauty","Toys","Garden","Auto","Food"],
        default=[], placeholder="All categories")

    st.markdown("---")
    st.markdown("<div style='color:#6B7280;font-size:10px;font-weight:600;"
                "letter-spacing:1.2px;text-transform:uppercase;margin-bottom:8px'>"
                "Pipeline Status</div>", unsafe_allow_html=True)

    for name, detail, col in [
        ("Kafka",     "3 topics · 100/s",      "#10B981"),
        ("PySpark",   "Streaming · 30s batch", "#10B981"),
        ("Snowflake", "RAW → ANALYTICS_DB",    "#10B981"),
        ("dbt",       "6 models · tests pass", "#10B981"),
        ("Airflow",   "DAG · daily 2am",       "#F59E0B"),
    ]:
        st.markdown(f"""
        <div style='display:flex;align-items:center;gap:8px;background:#111827;
                    border:1px solid #1E2535;border-radius:8px;
                    padding:8px 12px;margin-bottom:5px'>
          <div style='width:7px;height:7px;border-radius:50%;
                      background:{col};box-shadow:0 0 6px {col}'></div>
          <div>
            <div style='color:#E5E7EB;font-size:12px;font-weight:500'>{name}</div>
            <div style='color:#4B5563;font-size:10px'>{detail}</div>
          </div>
        </div>""", unsafe_allow_html=True)

    if not _live():
        st.markdown("""
        <div style='background:#1C1A12;border:1px solid #78350F;border-radius:8px;
                    padding:10px 12px;margin-top:8px'>
          <div style='color:#F59E0B;font-size:11px;font-weight:600'>DEMO MODE</div>
          <div style='color:#92400E;font-size:10px;margin-top:3px'>
            Set SNOWFLAKE_* in .env for live data
          </div>
        </div>""", unsafe_allow_html=True)


# ── Header ─────────────────────────────────────────────────────
hc1, hc2 = st.columns([6, 1])
with hc1:
    st.markdown(f"""
    <div style='padding:8px 0 4px'>
      <h1 style='color:#F9FAFB;font-size:26px;font-weight:700;
                 letter-spacing:-0.5px;margin:0'>User Behavior Analytics</h1>
      <p style='color:#6B7280;font-size:12px;margin:4px 0 0'>
        Lambda Architecture &nbsp;·&nbsp; Kafka → PySpark → Snowflake → dbt
        &nbsp;·&nbsp; Updated {datetime.now().strftime('%H:%M:%S')}
      </p>
    </div>""", unsafe_allow_html=True)
with hc2:
    st.markdown("""
    <div style='padding-top:18px;text-align:right'>
      <span class='badge-live'>● LIVE</span>
    </div>""", unsafe_allow_html=True)

st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)


# ── Load data ──────────────────────────────────────────────────
def load():
    if _live():
        w = (f"hour_timestamp >= DATEADD(day,{-days_back},CURRENT_TIMESTAMP()) "
             f"AND country IN {sin(country_filter)} "
             f"AND device  IN {sin(device_filter)}")
        return (
            qry(f"SELECT COALESCE(SUM(total_clicks),0) AS TOTAL_CLICKS,"
                f"COALESCE(SUM(unique_users),0) AS TOTAL_USERS,"
                f"COALESCE(SUM(total_orders),0) AS TOTAL_ORDERS,"
                f"COALESCE(SUM(total_revenue_usd),0) AS TOTAL_REVENUE,"
                f"COALESCE(AVG(conversion_rate_pct),0) AS AVG_CONVERSION_RATE "
                f"FROM AGG_HOURLY_KPIS WHERE {w}"),
            qry(f"SELECT DATE_TRUNC('day',hour_timestamp) AS DATE,"
                f"SUM(total_revenue_usd) AS DAILY_REVENUE "
                f"FROM AGG_HOURLY_KPIS WHERE {w} GROUP BY 1 ORDER BY 1"),
            qry(f"SELECT product_category AS PRODUCT_CATEGORY,"
                f"SUM(total_clicks) AS CLICKS "
                f"FROM AGG_HOURLY_KPIS WHERE {w} GROUP BY 1 ORDER BY 2 DESC LIMIT 10"),
            qry(f"SELECT device AS DEVICE,SUM(total_clicks) AS CLICKS "
                f"FROM AGG_HOURLY_KPIS WHERE {w} GROUP BY 1"),
            qry(f"SELECT hour_timestamp AS HOUR,"
                f"SUM(total_clicks) AS CLICKS,SUM(total_revenue_usd) AS REVENUE "
                f"FROM AGG_HOURLY_KPIS WHERE {w} GROUP BY 1 ORDER BY 1"),
        )

    fdf = _filt(_demo(), days_back, country_filter, device_filter)
    if cat_filter:
        fdf = fdf[fdf["product_category"].isin(cat_filter)]

    kpi = pd.DataFrame([{
        "TOTAL_CLICKS":        int(fdf["total_clicks"].sum()),
        "TOTAL_USERS":         int(fdf["unique_users"].sum()),
        "TOTAL_ORDERS":        int(fdf["total_orders"].sum()),
        "TOTAL_REVENUE":       float(fdf["total_revenue_usd"].sum()),
        "AVG_CONVERSION_RATE": float(fdf["conversion_rate_pct"].mean()),
    }]) if not fdf.empty else pd.DataFrame()

    daily = (fdf.groupby(pd.to_datetime(fdf["hour_timestamp"]).dt.normalize())
             ["total_revenue_usd"].sum().reset_index())
    daily.columns = ["DATE", "DAILY_REVENUE"]

    cat = (fdf.groupby("product_category", as_index=False)["total_clicks"]
           .sum().nlargest(10, "total_clicks"))
    cat.columns = ["PRODUCT_CATEGORY", "CLICKS"]

    dev = fdf.groupby("device", as_index=False)["total_clicks"].sum()
    dev.columns = ["DEVICE", "CLICKS"]

    hourly = (fdf.groupby("hour_timestamp", as_index=False)
              .agg(CLICKS=("total_clicks","sum"), REVENUE=("total_revenue_usd","sum"))
              .rename(columns={"hour_timestamp":"HOUR"})
              .sort_values("HOUR"))

    return kpi, daily, cat, dev, hourly


kpi_df, rev_df, cat_df, dev_df, hourly_df = load()


# ── KPI Cards ──────────────────────────────────────────────────
st.markdown("<div class='section-header'>Key Metrics</div>",
            unsafe_allow_html=True)

if not kpi_df.empty:
    r = kpi_df.iloc[0]
    cards = [
        ("Total Clicks",    fmt(r.get("TOTAL_CLICKS",0)),            "▲ 12.4%", "#3B82F6"),
        ("Unique Users",    fmt(r.get("TOTAL_USERS",0)),             "▲ 8.1%",  "#8B5CF6"),
        ("Total Orders",    fmt(r.get("TOTAL_ORDERS",0)),            "▲ 5.7%",  "#10B981"),
        ("Revenue",         f"${fmt(r.get('TOTAL_REVENUE',0))}",     "▲ 18.3%", "#F59E0B"),
        ("Conversion Rate", f"{float(r.get('AVG_CONVERSION_RATE',0)):.2f}%", "▲ 0.4pp", "#EF4444"),
    ]
    cols = st.columns(5)
    for i, (label, value, delta, color) in enumerate(cards):
        with cols[i]:
            st.markdown(f"""
            <div class='kpi-card'>
              <div style='position:absolute;top:0;left:0;right:0;height:3px;
                          background:{color};border-radius:16px 16px 0 0'></div>
              <div class='kpi-label'>{label}</div>
              <div class='kpi-value' style='color:{color}'>{value}</div>
              <div class='kpi-delta'>
                <span style='color:#10B981'>{delta}</span>
                <span style='color:#4B5563;font-size:10px'> vs prev period</span>
              </div>
            </div>""", unsafe_allow_html=True)

st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

# ── Row 2: Revenue + Category ──────────────────────────────────
st.markdown("<div class='section-header'>Revenue & Engagement</div>",
            unsafe_allow_html=True)
r1, r2 = st.columns([3, 2])

with r1:
    if not rev_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=rev_df.iloc[:,0], y=rev_df.iloc[:,1],
            mode="lines",
            line=dict(color="#3B82F6", width=2.5),
            fill="tozeroy",
            fillcolor="rgba(59,130,246,0.07)",
            hovertemplate="<b>%{x|%b %d}</b><br>$%{y:,.0f}<extra></extra>",
            name="Revenue",
        ))
        fig.update_layout(L("Daily Revenue (USD)"))
        st.plotly_chart(fig, use_container_width=True)

with r2:
    if not cat_df.empty:
        fig = px.bar(
            cat_df, x="CLICKS", y="PRODUCT_CATEGORY",
            orientation="h",
            color="CLICKS",
            color_continuous_scale=["#1E3A5F","#3B82F6","#93C5FD"],
        )
        lay = L("Clicks by Category")
        lay["yaxis"]["categoryorder"] = "total ascending"
        lay["coloraxis_showscale"] = False
        fig.update_layout(lay)
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>%{x:,} clicks<extra></extra>")
        st.plotly_chart(fig, use_container_width=True)

# ── Row 3: Hourly + Device ─────────────────────────────────────
st.markdown("<div class='section-header'>Traffic Patterns</div>",
            unsafe_allow_html=True)
r3, r4 = st.columns([3, 2])

with r3:
    if not hourly_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=hourly_df["HOUR"], y=hourly_df["CLICKS"],
            marker_color="rgba(59,130,246,0.55)",
            name="Clicks",
            hovertemplate="<b>%{x}</b><br>%{y:,} clicks<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=hourly_df["HOUR"], y=hourly_df["REVENUE"],
            mode="lines", yaxis="y2",
            line=dict(color="#10B981", width=2),
            name="Revenue ($)",
            hovertemplate="$%{y:,.0f}<extra></extra>",
        ))
        fig.update_layout(L("Hourly Clicks & Revenue", y2=True, hleg=True))
        st.plotly_chart(fig, use_container_width=True)

with r4:
    if not dev_df.empty:
        fig = go.Figure(go.Pie(
            labels=dev_df["DEVICE"],
            values=dev_df["CLICKS"],
            hole=0.64,
            marker=dict(
                colors=["#3B82F6","#8B5CF6","#10B981"],
                line=dict(color="#0A0E1A", width=3),
            ),
            textfont=dict(color="#E5E7EB", size=12),
            hovertemplate="<b>%{label}</b><br>%{value:,} · %{percent}<extra></extra>",
        ))
        total = int(dev_df["CLICKS"].sum())
        fig.add_annotation(
            text=f"<b>{fmt(total)}</b>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color="#F9FAFB", size=22),
        )
        lay = L("Device Split")
        lay["showlegend"] = True
        fig.update_layout(lay)
        st.plotly_chart(fig, use_container_width=True)

# ── Footer ─────────────────────────────────────────────────────
st.markdown("---")
fc1, fc2, fc3 = st.columns([3, 3, 1])
with fc1:
    st.markdown("""<div style='color:#374151;font-size:11px'>
      Kafka · PySpark · Snowflake · dbt · Streamlit · Lambda Architecture
    </div>""", unsafe_allow_html=True)
with fc2:
    st.markdown(f"""<div style='color:#374151;font-size:11px'>
      Auto-refresh 60s &nbsp;·&nbsp;
      Last <b style='color:#4B5563'>{days_back}d</b> &nbsp;·&nbsp;
      <b style='color:#4B5563'>{len(country_filter)}</b> countries
    </div>""", unsafe_allow_html=True)
with fc3:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()