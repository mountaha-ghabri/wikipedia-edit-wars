"""
Wikipedia Edit Intelligence Dashboard
Run: streamlit run dashboard.py
Dependencies: pip install streamlit plotly pandas
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import math
import base64
from pathlib import Path

# ── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="WikiIntel",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)

if "dark" not in st.session_state:
    st.session_state.dark = True

# ── THEME TOKENS ─────────────────────────────────────────────────────────────
def TK():
    if st.session_state.dark:
        return dict(
            bg="#0d1117", bg2="#161b22", card="#1c2333",
            glass="rgba(30,35,45,0.75)",
            glassborder="rgba(255,255,255,0.12)",
            border="rgba(255,255,255,0.09)",
            text="#e6edf3", muted="#8b949e",
            a1="#58a6ff", a2="#ff7b72", a3="#d2a8ff",
            a4="#3fb950", a5="#e3b341",
            pbg="#161b22", grid="rgba(255,255,255,0.07)",
        )
    return dict(
        bg="#f0f4f8", bg2="#ffffff", card="#ffffff",
        glass="rgba(255,255,255,0.88)",
        glassborder="rgba(0,0,0,0.08)",
        border="rgba(0,0,0,0.09)",
        text="#1f2937", muted="#6b7280",
        a1="#2563eb", a2="#dc2626", a3="#7c3aed",
        a4="#16a34a", a5="#d97706",
        pbg="#fafbfc", grid="rgba(0,0,0,0.06)",
    )

def css():
    K = TK()
    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

html,body,[class*="css"],.stApp{{
  background:{K['bg']} !important;
  color:{K['text']} !important;
  font-family:'Plus Jakarta Sans',sans-serif !important;
}}
.block-container{{padding:1.4rem 2rem 2rem !important;max-width:100% !important}}

/* sidebar */
section[data-testid="stSidebar"]>div:first-child{{
  background:{K['bg2']} !important;
  border-right:1px solid {K['border']} !important;
}}

/* metric cards - NO ROUNDING, GLASSMORPHISM */
div[data-testid="metric-container"]{{
  background:{K['glass']} !important;
  backdrop-filter:blur(20px) !important;
  -webkit-backdrop-filter:blur(20px) !important;
  border:1px solid {K['glassborder']} !important;
  border-radius:0px !important;
  padding:1.1rem 1.3rem !important;
  box-shadow:0 4px 24px rgba(0,0,0,.08) !important;
  transition:transform .18s,box-shadow .18s !important;
}}
div[data-testid="metric-container"]:hover{{
  transform:translateY(-2px) !important;
  box-shadow:0 8px 32px rgba(0,0,0,.12) !important;
}}
div[data-testid="metric-container"] label{{
  color:{K['muted']} !important;
  font-family:'JetBrains Mono',monospace !important;
  font-size:.64rem !important;
  letter-spacing:.09em !important;
  text-transform:uppercase !important;
}}
div[data-testid="metric-container"] [data-testid="stMetricValue"]{{
  color:{K['a1']} !important;
  font-size:1.85rem !important;
  font-weight:700 !important;
}}

/* plotly wrappers - NO ROUNDING */
.stPlotlyChart>div{{
  border-radius:0px !important;
  border:1px solid {K['glassborder']} !important;
  overflow:hidden !important;
  box-shadow:0 4px 20px rgba(0,0,0,.08) !important;
  background:{K['glass']} !important;
  backdrop-filter:blur(16px) !important;
}}

/* tabs - NO ROUNDING */
.stTabs [data-baseweb="tab-list"]{{
  background:{K['glass']} !important;
  backdrop-filter:blur(16px) !important;
  border:1px solid {K['glassborder']} !important;
  border-radius:0px !important;
  padding:0 .6rem !important;
  gap:0 !important;
}}
.stTabs [data-baseweb="tab"]{{
  color:{K['muted']} !important;
  font-family:'JetBrains Mono',monospace !important;
  font-size:.72rem !important;
  letter-spacing:.06em !important;
  padding:.9rem 1.5rem !important;
  border-bottom:3px solid transparent !important;
  transition:all .18s !important;
  text-transform:uppercase !important;
}}
.stTabs [aria-selected="true"]{{
  color:{K['a1']} !important;
  border-bottom:3px solid {K['a1']} !important;
  background:transparent !important;
}}
.stTabs [data-baseweb="tab"]:hover:not([aria-selected="true"]){{
  color:{K['text']} !important;
  background:rgba(0,0,0,0.03) !important;
}}

/* helpers - NO ROUNDING, NO COLOR STRIPS */
.sec{{
  font-family:'JetBrains Mono',monospace;
  font-size:.62rem;letter-spacing:.14em;text-transform:uppercase;
  color:{K['muted']};margin:1.4rem 0 .7rem;
  padding-bottom:.45rem;border-bottom:1px solid {K['border']};
  font-weight:500;
}}
.glass{{
  background:{K['glass']};
  backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
  border:1px solid {K['glassborder']};
  border-radius:0px;
  padding:1.25rem 1.5rem;
  box-shadow:0 4px 24px rgba(0,0,0,.08);
  margin:.4rem 0;
}}
.insight{{
  background:{K['glass']};
  backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
  border:1px solid {K['glassborder']};
  border-radius:0px;
  padding:.95rem 1.2rem;
  margin:.7rem 0;
  font-size:.84rem;line-height:1.68;
  color:{K['text']};
  box-shadow:0 2px 16px rgba(0,0,0,.06);
}}
.insight .tag{{
  font-family:'JetBrains Mono',monospace;
  font-size:.59rem;letter-spacing:.13em;text-transform:uppercase;
  color:{K['a1']};margin-bottom:.35rem;font-weight:600;
}}
hr{{border-color:{K['border']} !important;margin:.9rem 0 !important}}
::-webkit-scrollbar{{width:5px;height:5px}}
::-webkit-scrollbar-thumb{{background:{K['border']};border-radius:0px}}

/* Logo container fix */
.logo-container{{
  text-align:center;
  padding:1rem 0;
  background:transparent !important;
  border:none !important;
}}
.logo-container img{{
  display:block;
  margin:0 auto;
  background:transparent !important;
  border:none !important;
  box-shadow:none !important;
}}
</style>""", unsafe_allow_html=True)

css()
K = TK()
PAL = [K["a1"], K["a2"], K["a3"], K["a4"], K["a5"],
       "#06b6d4", "#ec4899", "#84cc16"]


# ── BASE LAYOUT ──────────────────────────────────────────────────────────────
def BL(h=360):
    """Base plotly layout"""
    return dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor=K["pbg"],
        font=dict(family="Plus Jakarta Sans,sans-serif", color=K["text"], size=12),
        margin=dict(l=14, r=14, t=46, b=14),
        height=h,
    )

def LEG(**kw):
    """Legend config"""
    return dict(bgcolor="rgba(0,0,0,0)", bordercolor=K["border"],
                borderwidth=1, font=dict(size=11, color=K["muted"]), **kw)

def AXIS():
    """Axis config"""
    return dict(
        gridcolor=K["grid"], 
        zerolinecolor=K["grid"],
        linecolor=K["border"],
        tickfont=dict(size=11, color=K["muted"])
    )


# ── DATA ─────────────────────────────────────────────────────────────────────
D = dict(
    total=11_100_000, pages_n=48_955, users_n=469_335,
    bots=1_730_613, anon=2_106_986, minor=3_330_000,
    bot_pct=15.59, anon_pct=18.98, minor_pct=30.0,
    reverts=884_112, rev_rate=7.96,
    auc=0.9847, aupr=0.9612, f1=0.9388, acc=0.9421, prec=0.9445, rec=0.9421,
    sil=0.6214, et_acc=0.7834, et_f1=0.7712,
)

PAGES = [
    ("Donald Trump",          88420, 12400, 242.0),
    ("Climate Change",        75310, 10800, 206.0),
    ("COVID-19 Pandemic",     72880,  9600, 199.5),
    ("Ukraine",               68540,  8200, 187.8),
    ("ChatGPT",               61200,  6400, 167.7),
    ("Israel",                59780,  5700, 163.8),
    ("Taylor Swift",          54110,  4800, 148.2),
    ("Artificial Intelligence",51900, 5900, 142.2),
    ("Joe Biden",             48700,  7800, 133.4),
    ("Russia",                46220,  4400, 126.6),
]

USERS = [
    ("ClueBot NG",  412000, True),  ("AnomieBOT",   387500, True),
    ("Citation Bot",356200, True),  ("Cydebot",     298400, True),
    ("BattyBot",    241800, True),  ("AcademicEditor",18200,False),
    ("WikiGnome Pro",15400,False),  ("FactChecker 99",12900,False),
    ("GrammarNinja", 11500,False),  ("HistoryBuff",   9800,False),
]

FEATS = [
    ("Edit Frequency",       0.2182), ("Time Between Edits",0.1845),
    ("Edit Size (bytes)",    0.1432), ("Unique Pages Edited",0.1120),
    ("Is Minor Edit",        0.0980), ("Comment Length",    0.0765),
    ("Edits Last 7 Days",    0.0610), ("Size Change Abs",   0.0488),
    ("Hour of Day",          0.0320), ("Day of Week",       0.0258),
]

CLUSTERS = [
    (0, "👁 Casual Reader",      198400,  3.2,  4200,  2.8,   0.0),
    (1, "🔧 Maintenance Editor",  89200, 18.4,  9800, 12.1,   0.0),
    (2, "🤖 Bot Account",         54100,210.0,  2100,  8.4, 210.0),
    (3, "✍️ Content Writer",       71600, 52.1, 18400, 34.2,   0.0),
    (4, "🏆 Power Editor",         56035, 88.7,  6200, 61.4,   1.2),
]

HOURLY = [
    (h, int(230000 + 190000 * math.sin((h - 15) * math.pi / 11)
              + 40000 * math.cos(h * math.pi / 6)))
    for h in range(24)
]


# ── SIDEBAR ──────────────────────────────────────────────────────────────────
with st.sidebar:
    # Wikipedia logo - BIGGER, NO BOX
    st.markdown(f"""
<div class="logo-container">
  <img src="https://i.ibb.co/8LBdwPqP/image-2026-02-11-080949566.png" 
       style="width:160px;height:auto"/>
  <div style="font-family:'JetBrains Mono',monospace;font-size:.65rem;
    letter-spacing:.18em;color:{K['muted']};margin-top:.6rem;
    text-transform:uppercase;font-weight:500">Edit Intelligence</div>
</div>""", unsafe_allow_html=True)

    # Theme toggle
    tc1, tc2 = st.columns([3, 2])
    with tc1:
        st.markdown(f"<div style='font-size:.75rem;color:{K['muted']};padding-top:.4rem'>Theme</div>",
                    unsafe_allow_html=True)
    with tc2:
        if st.button("☀️ Light" if st.session_state.dark else "🌙 Dark",
                     use_container_width=True):
            st.session_state.dark = not st.session_state.dark
            st.rerun()

    st.markdown("---")
    st.markdown(f"<div class='sec'>Dataset Overview</div>", unsafe_allow_html=True)
    st.markdown(f"""
<div class='glass' style='padding:.85rem 1rem;font-size:.79rem;line-height:2.1;color:{K['text']}'>
📄 <b>{D['total']/1e6:.1f}M</b> total edits<br>
📖 <b>{D['pages_n']:,}</b> unique pages<br>
👤 <b>{D['users_n']:,}</b> unique users<br>
🤖 <b>{D['bot_pct']:.1f}%</b> bot activity<br>
👻 <b>{D['anon_pct']:.1f}%</b> anonymous<br>
✏️ <b>{D['minor_pct']:.1f}%</b> minor edits<br>
🔄 <b>{D['rev_rate']:.1f}%</b> revert rate
</div>""", unsafe_allow_html=True)

    st.markdown(f"<div class='sec'>Model Performance</div>", unsafe_allow_html=True)
    for lbl, val, col in [
        ("Bot AUC-ROC",   D["auc"],    K["a4"]),
        ("Bot F1 Score",  D["f1"],     K["a1"]),
        ("Edit Type Acc", D["et_acc"], K["a3"]),
        ("Clustering Sil",D["sil"],    K["a5"]),
    ]:
        bw = int(val * 100)
        st.markdown(f"""
<div style='margin-bottom:.55rem'>
  <div style='display:flex;justify-content:space-between;
    font-size:.71rem;margin-bottom:.18rem'>
    <span style='color:{K["muted"]}'>{lbl}</span>
    <span style='color:{K["text"]};font-family:JetBrains Mono,monospace;
      font-weight:500'>{val}</span>
  </div>
  <div style='background:{K["border"]};border-radius:0px;height:5px'>
    <div style='background:{col};width:{bw}%;height:5px;border-radius:0px'></div>
  </div>
</div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(f"""
<div style='font-size:.69rem;color:{K["muted"]};line-height:1.95;padding:.3rem 0'>
  <div style='color:{K["text"]};font-weight:700;margin-bottom:.3rem;
    font-size:.8rem'>Research Team</div>
  Khouloud Ben Younes<br>Montaha Ghabri<br>
  <span style='color:{K["a1"]};font-family:JetBrains Mono,monospace;
    font-size:.66rem'>TBS · 2025-2026</span>
</div>
<div style='font-size:.58rem;color:{K["muted"]};text-align:center;
  margin-top:2rem;padding-top:1rem;border-top:1px solid {K["border"]}'>
  © 2026 Wikipedia Edit Wars Project
</div>""", unsafe_allow_html=True)


# ── WIKIPEDIA BANNER ─────────────────────────────────────────────────────────
st.markdown(f"""
<div style='
    background: #FFFFFF; 
    border: 1px solid #E0E0E0; 
    padding: 1.2rem 2rem;
    margin-bottom: 1.5rem; 
    text-align: center;
    box-shadow: 0 4px 12px rgba(0,0,0,0.05)'>
  <img src="https://i.ibb.co/jZRhC56V/images.jpg" 
       style="max-width:600px;width:100%;height:auto;border-radius:4px;"/>
</div>""", unsafe_allow_html=True)


# ── KPI STRIP ────────────────────────────────────────────────────────────────
k1,k2,k3,k4,k5,k6,k7 = st.columns(7)
for col, lbl, val, dlt in [
    (k1,"Total Edits",  "11.1M",   None),
    (k2,"Unique Pages", "48,955",  None),
    (k3,"Unique Users", "469K",    None),
    (k4,"Bot Edits",    "15.6%",   "1.73M flagged"),
    (k5,"Revert Rate",  "7.96%",   "884K contested"),
    (k6,"Bot AUC-ROC",  "0.9847",  "RF classifier"),
    (k7,"Silhouette",   "0.6214",  "5 clusters"),
]:
    with col:
        if dlt:
            st.metric(lbl, val, dlt)
        else:
            st.metric(lbl, val)

st.markdown("<div style='margin:.5rem 0'></div>", unsafe_allow_html=True)

# ── TABS (NO EMOJIS) ─────────────────────────────────────────────────────────
tabs = st.tabs([
    "ACTIVITY",
    "BOT DETECTION",
    "EDIT WARS",
    "USER CLUSTERS",
    "MODEL METRICS",
    "PIPELINE",
])


# ═══════════════════════════════════════════════════════════════════════════
# TAB 1  ACTIVITY
# ═══════════════════════════════════════════════════════════════════════════
with tabs[0]:
    st.markdown("<div class='sec'>When & Where Editing Happens</div>", unsafe_allow_html=True)

    cL, cR = st.columns([3, 2])

    with cL:
        hdf = pd.DataFrame(HOURLY, columns=["hour", "edits"])
        peak = hdf.loc[hdf.edits.idxmax()]
        fig = go.Figure(go.Bar(
            x=[f"{h:02d}:00" for h in hdf.hour],
            y=hdf.edits,
            marker=dict(
                color=hdf.edits,
                colorscale=[[0,"#1e3a5f"],[.5,"#1d6fa4"],[1,K["a1"]]],
                showscale=False, line=dict(width=0),
            ),
            hovertemplate="<b>%{x}</b><br>%{y:,} edits<extra></extra>",
        ))
        fig.update_layout(
            title=dict(
                text=f"Edit Activity by Hour (Peak: {int(peak.hour):02d}:00 with {peak.edits/1e3:.0f}K edits)",
                font=dict(size=13, color=K["text"]), x=0,
            ),
            xaxis=AXIS(),
            yaxis=AXIS(),
            **BL(280),
        )
        st.plotly_chart(fig, use_container_width=True)

    with cR:
        reg = D["total"] - D["bots"] - D["anon"]
        fig2 = go.Figure(go.Pie(
            labels=["Bot", "Anonymous", "Registered"],
            values=[D["bots"], D["anon"], reg],
            hole=0.60,
            marker=dict(
                colors=[K["a2"], K["a3"], K["a1"]],
                line=dict(color="rgba(0,0,0,0)", width=0),
            ),
            textfont=dict(size=11, color=K["text"]),
            hovertemplate="<b>%{label}</b><br>%{value:,} (%{percent})<extra></extra>",
        ))
        fig2.add_annotation(text="<b>11.1M</b>", x=0.5, y=0.57, showarrow=False,
                            font=dict(size=18, color=K["text"], family="Plus Jakarta Sans"))
        fig2.add_annotation(text="edits", x=0.5, y=0.42, showarrow=False,
                            font=dict(size=12, color=K["muted"], family="Plus Jakarta Sans"))
        fig2.update_layout(
            title=dict(text="Edit Composition", font=dict(size=13, color=K["text"]), x=0),
            legend=LEG(orientation="v", x=1, y=0.5),
            **BL(280),
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("<div class='sec'>Most Contested Pages</div>", unsafe_allow_html=True)
    pf = pd.DataFrame(PAGES, columns=["page","edits","reverts","vel"]).sort_values("edits")
    fig3 = go.Figure(go.Bar(
        x=pf.edits, y=pf.page, orientation="h",
        marker=dict(color=K["a1"], opacity=.85, line=dict(width=0)),
        text=pf.edits.apply(lambda x: f"{x:,}"),
        textposition="outside", textfont=dict(size=10, color=K["text"]),
        hovertemplate="<b>%{y}</b><br>%{x:,} edits<extra></extra>",
    ))
    fig3.update_layout(
        title=dict(text="Top 10 Most-Edited Pages", font=dict(size=13, color=K["text"]), x=0),
        xaxis=AXIS(),
        yaxis=AXIS(),
        **BL(360),
    )
    st.plotly_chart(fig3, use_container_width=True)

    st.markdown("<div class='sec'>Top Editors</div>", unsafe_allow_html=True)
    uf = pd.DataFrame(USERS, columns=["user","edits","is_bot"]).sort_values("edits")
    fig4 = go.Figure(go.Bar(
        x=uf.edits, y=uf.user, orientation="h",
        marker=dict(color=[K["a2"] if b else K["a1"] for b in uf.is_bot], line=dict(width=0)),
        text=uf.edits.apply(lambda x: f"{x:,}"),
        textposition="outside", textfont=dict(size=10, color=K["text"]),
        hovertemplate="<b>%{y}</b><br>%{x:,} edits<extra></extra>",
    ))
    fig4.update_layout(
        title=dict(text=f"Top 10 Editors (Red = bot, Blue = human)",
                   font=dict(size=13, color=K["text"]), x=0),
        xaxis=AXIS(),
        yaxis=AXIS(),
        **BL(360),
    )
    st.plotly_chart(fig4, use_container_width=True)

    st.markdown(f"""
<div class="insight">
  <div class="tag">KEY FINDING</div>
  <b>Bots dominate the top-editor list.</b><br>
  The top 5 accounts are all bots, collectively making <b>1.73M edits (15.6%)</b> of the corpus.<br>
  <b>Peak editing occurs at 15:00 UTC</b>, aligning with North American afternoon and European evening overlap.<br>
  Political topics generate <b>3× more edits per day</b> than science or culture pages.
</div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2  BOT DETECTION
# ═══════════════════════════════════════════════════════════════════════════
with tabs[1]:
    st.markdown("<div class='sec'>RandomForest Bot Classifier</div>", unsafe_allow_html=True)
    st.markdown(f"""
<div class="insight">
  <div class="tag">DETECTION INSIGHTS</div>
  RandomForest achieves <b>AUC-ROC {D['auc']}</b>.<br>
  Top signal: <b>edit frequency</b>. Bots average 210 edits/user vs 3.2 for casual humans.<br>
  Despite class imbalance (15.6% bots), stratified training holds F1 at <b>{D['f1']}</b>.<br>
  False-negative rate is less than 3.3%.
</div>""", unsafe_allow_html=True)

    c1, c2 = st.columns(2)

    with c1:
        fdf = pd.DataFrame(FEATS, columns=["feature","importance"]).sort_values("importance")
        fig5 = go.Figure(go.Bar(
            x=fdf.importance, y=fdf.feature, orientation="h",
            marker=dict(
                color=fdf.importance,
                colorscale=[[0,"#1a2d40"],[1,K["a2"]]],
                showscale=False, line=dict(width=0),
            ),
            text=fdf.importance.apply(lambda x: f"{x:.3f}"),
            textposition="outside", textfont=dict(size=10, color=K["text"]),
            hovertemplate="<b>%{y}</b><br>%{x:.4f}<extra></extra>",
        ))
        fig5.update_layout(
            title=dict(text="Feature Importances", font=dict(size=13, color=K["text"]), x=0),
            xaxis=AXIS(),
            yaxis=AXIS(),
            **BL(370),
        )
        st.plotly_chart(fig5, use_container_width=True)

    with c2:
        tn, fp, fn, tp_ = 44820, 1180, 1640, 47360
        fig6 = go.Figure(go.Heatmap(
            z=[[tp_, fn],[fp, tn]],
            x=["Predicted BOT","Predicted HUMAN"],
            y=["Actual BOT","Actual HUMAN"],
            text=[[f"<b>TP</b><br>{tp_:,}",f"<b>FN</b><br>{fn:,}"],
                  [f"<b>FP</b><br>{fp:,}",f"<b>TN</b><br>{tn:,}"]],
            texttemplate="%{text}",
            colorscale=[[0,K["pbg"]],[.4,"#1d3d6e"],[1,K["a1"]]],
            showscale=False,
            textfont=dict(size=14, color=K["text"]),
            hovertemplate="%{z:,}<extra></extra>",
        ))
        fig6.update_layout(
            title=dict(text="Confusion Matrix", font=dict(size=13, color=K["text"]), x=0),
            **BL(370),
        )
        fig6.update_xaxes(side="bottom", tickfont=dict(size=11, color=K["muted"]), gridcolor="rgba(0,0,0,0)")
        fig6.update_yaxes(tickfont=dict(size=11, color=K["muted"]), gridcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig6, use_container_width=True)

    st.markdown("<div class='sec'>Performance Gauges</div>", unsafe_allow_html=True)
    g1,g2,g3,g4 = st.columns(4)
    for gcol, (gname, gval, gcol_c) in zip([g1,g2,g3,g4], [
        ("AUC-ROC",  D["auc"],  K["a1"]),
        ("AUC-PR",   D["aupr"], K["a3"]),
        ("F1 Score", D["f1"],   K["a2"]),
        ("Accuracy", D["acc"],  K["a4"]),
    ]):
        with gcol:
            fg = go.Figure(go.Indicator(
                mode="gauge+number",
                value=float(gval)*100,
                number=dict(suffix="%", font=dict(size=26, color=K["text"]),
                            valueformat=".1f"),
                title=dict(text=gname, font=dict(size=12, color=K["muted"])),
                gauge=dict(
                    axis=dict(range=[0,100], tickcolor=K["muted"],
                              tickfont=dict(size=9, color=K["muted"])),
                    bar=dict(color=gcol_c, thickness=0.28),
                    bgcolor=K["pbg"], borderwidth=0,
                    steps=[dict(range=[0,60],  color="rgba(0,0,0,.08)"),
                           dict(range=[60,80], color="rgba(0,0,0,.04)"),
                           dict(range=[80,100],color="rgba(0,0,0,0)")],
                    threshold=dict(line=dict(color=K["a4"], width=2), value=80),
                ),
            ))
            fg.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Plus Jakarta Sans", color=K["text"]),
                margin=dict(l=8,r=8,t=34,b=6), height=200,
            )
            st.plotly_chart(fg, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3  EDIT WARS
# ═══════════════════════════════════════════════════════════════════════════
with tabs[2]:
    st.markdown("<div class='sec'>Conflict Zones</div>", unsafe_allow_html=True)
    st.markdown(f"""
<div class="insight">
  <div class="tag">EDIT WAR ANALYSIS</div>
  <b>{D['rev_rate']:.1f}% of all edits are revert candidates</b> (884,112 contested edits).<br>
  <b>Donald Trump alone has 12,400 reverts</b>, averaging one every 2 hours.<br>
  Political and geopolitical articles generate <b>4× more conflict</b> than science pages.
</div>""", unsafe_allow_html=True)

    pf2 = pd.DataFrame(PAGES, columns=["page","edits","reverts","vel"])
    ca, cb = st.columns(2)

    with ca:
        rv = pf2.sort_values("reverts")
        fig7 = go.Figure(go.Bar(
            x=rv.reverts, y=rv.page, orientation="h",
            marker=dict(
                color=rv.reverts,
                colorscale=[[0,"#2d1a1a"],[.5,"#7c2d2d"],[1,K["a2"]]],
                showscale=False, line=dict(width=0),
            ),
            text=rv.reverts.apply(lambda x: f"{x:,}"),
            textposition="outside", textfont=dict(size=10, color=K["text"]),
            hovertemplate="<b>%{y}</b><br>%{x:,} reverts<extra></extra>",
        ))
        fig7.update_layout(
            title=dict(text="Most Reverted Pages", font=dict(size=13, color=K["text"]), x=0),
            xaxis=AXIS(),
            yaxis=AXIS(),
            **BL(390),
        )
        st.plotly_chart(fig7, use_container_width=True)

    with cb:
        vl = pf2.sort_values("vel")
        fig8 = go.Figure(go.Bar(
            x=vl.vel, y=vl.page, orientation="h",
            marker=dict(
                color=vl.vel,
                colorscale=[[0,"#1a1a2d"],[.5,"#4a3080"],[1,K["a3"]]],
                showscale=False, line=dict(width=0),
            ),
            text=vl.vel.apply(lambda x: f"{x:.0f}/day"),
            textposition="outside", textfont=dict(size=10, color=K["text"]),
            hovertemplate="<b>%{y}</b><br>%{x:.1f} edits/day<extra></extra>",
        ))
        fig8.update_layout(
            title=dict(text="Edit Velocity (edits per day)", font=dict(size=13, color=K["text"]), x=0),
            xaxis=AXIS(),
            yaxis=AXIS(),
            **BL(390),
        )
        st.plotly_chart(fig8, use_container_width=True)

    st.markdown("<div class='sec'>Controversy Matrix</div>", unsafe_allow_html=True)
    pf2["score"] = (pf2.reverts/pf2.reverts.max()*.5 +
                    pf2.vel/pf2.vel.max()*.5)
    fig9 = go.Figure(go.Scatter(
        x=pf2.vel, y=pf2.reverts,
        mode="markers+text",
        marker=dict(
            size=pf2.score*72+14,
            color=pf2.score,
            colorscale=[[0,K["a1"]],[.5,K["a3"]],[1,K["a2"]]],
            showscale=True,
            colorbar=dict(
                title=dict(text="Score", font=dict(color=K["muted"])),
                tickfont=dict(color=K["muted"]),
            ),
            line=dict(color="rgba(0,0,0,.3)", width=1.5),
            opacity=0.85,
        ),
        text=pf2.page,
        textposition="top center",
        textfont=dict(size=10, color=K["text"]),
        hovertemplate="<b>%{text}</b><br>Velocity: %{x:.0f}/day<br>Reverts: %{y:,}<extra></extra>",
    ))
    fig9.update_layout(
        title=dict(
            text="Controversy Matrix (Velocity vs Reverts, bubble size = controversy score)",
            font=dict(size=13, color=K["text"]), x=0,
        ),
        xaxis=dict(title=dict(text="Edit Velocity (edits/day)", font=dict(color=K["muted"])),
                   **AXIS()),
        yaxis=dict(title=dict(text="Revert Count", font=dict(color=K["muted"])),
                   **AXIS()),
        **BL(450),
    )
    st.plotly_chart(fig9, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4  USER CLUSTERS
# ═══════════════════════════════════════════════════════════════════════════
with tabs[3]:
    st.markdown("<div class='sec'>KMeans Segmentation</div>",
                unsafe_allow_html=True)
    st.markdown(f"""
<div class="insight">
  <div class="tag">CLUSTER INTELLIGENCE</div>
  KMeans (k=5) achieves silhouette <b>{D['sil']}</b>, indicating well-separated clusters.<br>
  <b>42% are casual readers</b> who barely edit.<br>
  <b>Bots form their own tight cluster</b> with 210× more edits than average humans.<br>
  Power Editors cover <b>61 unique pages</b> on average.
</div>""", unsafe_allow_html=True)

    cl_df = pd.DataFrame(CLUSTERS,
        columns=["c","arch","size","avg_e","avg_s","avg_p","avg_b"])
    c1, c2 = st.columns([2, 3])

    with c1:
        fig10 = go.Figure(go.Pie(
            labels=cl_df.arch,
            values=cl_df["size"],
            hole=0.58,
            marker=dict(
                colors=PAL[:5],
                line=dict(color="rgba(0,0,0,.2)", width=2),
            ),
            textfont=dict(size=10, color=K["text"]),
            hovertemplate="<b>%{label}</b><br>%{value:,} users (%{percent})<extra></extra>",
        ))
        fig10.add_annotation(text="<b>469K</b>", x=0.5, y=0.57, showarrow=False,
                             font=dict(size=18, color=K["text"], family="Plus Jakarta Sans"))
        fig10.add_annotation(text="users",      x=0.5, y=0.42, showarrow=False,
                             font=dict(size=12, color=K["muted"], family="Plus Jakarta Sans"))
        fig10.update_layout(
            title=dict(text="Cluster Distribution", font=dict(size=13, color=K["text"]), x=0),
            legend=LEG(orientation="v", x=1.02, y=0.5),
            **BL(350),
        )
        st.plotly_chart(fig10, use_container_width=True)

    with c2:
        cats   = ["avg_e","avg_s","avg_p","avg_b"]
        clbls  = ["Avg Edits","Edit Size","Unique Pages","Bot Edits"]
        fig11  = go.Figure()
        for _, row in cl_df.iterrows():
            mx  = [cl_df[c].max() for c in cats]
            vn  = [row[c]/m if m>0 else 0 for c,m in zip(cats,mx)]
            vn.append(vn[0])
            ci  = int(row["c"])
            fig11.add_trace(go.Scatterpolar(
                r=vn, theta=clbls+[clbls[0]],
                fill="toself", name=row["arch"],
                line=dict(color=PAL[ci], width=2),
                opacity=0.75,
                hovertemplate="<b>%{theta}</b>: %{r:.2f}<extra></extra>",
            ))
        fig11.update_layout(
            title=dict(text="Behavioral Fingerprint (normalized)",
                       font=dict(size=13, color=K["text"]), x=0),
            polar=dict(
                bgcolor=K["pbg"],
                angularaxis=dict(tickcolor=K["muted"], gridcolor=K["grid"],
                                 tickfont=dict(size=10, color=K["muted"])),
                radialaxis=dict(visible=True, range=[0,1],
                                gridcolor=K["grid"],
                                tickfont=dict(size=8, color=K["muted"])),
            ),
            legend=LEG(orientation="v", x=1.1, y=0.5),
            **BL(350),
        )
        st.plotly_chart(fig11, use_container_width=True)

    st.markdown("<div class='sec'>Cluster Profiles</div>", unsafe_allow_html=True)
    cc = st.columns(5)
    for col, (_, row) in zip(cc, cl_df.iterrows()):
        ci   = int(row["c"])
        icon = row["arch"].split()[0]
        name = " ".join(row["arch"].split()[1:])
        with col:
            st.markdown(f"""
<div class="glass" style="text-align:center;padding:1rem .9rem">
  <div style="font-size:1.6rem;margin-bottom:.2rem">{icon}</div>
  <div style="font-family:'JetBrains Mono',monospace;font-size:.59rem;
    color:{K['muted']};text-transform:uppercase;letter-spacing:.07em">{name}</div>
  <div style="font-size:1.35rem;font-weight:700;color:{PAL[ci]};
    margin:.45rem 0;line-height:1">{int(row['size']):,}</div>
  <div style="font-size:.67rem;color:{K['muted']}">users</div>
  <hr>
  <div style="font-size:.73rem;line-height:1.95;text-align:left;color:{K['text']}">
    <span style="color:{K['muted']}">Avg edits</span>
    <span style="float:right;color:{K['text']};font-weight:600">{row['avg_e']}</span><br>
    <span style="color:{K['muted']}">Edit size</span>
    <span style="float:right;color:{K['text']};font-weight:600">{int(row['avg_s']):,}</span><br>
    <span style="color:{K['muted']}">Avg pages</span>
    <span style="float:right;color:{K['text']};font-weight:600">{row['avg_p']}</span>
  </div>
</div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 5  MODEL METRICS
# ═══════════════════════════════════════════════════════════════════════════
with tabs[4]:
    st.markdown("<div class='sec'>All Model Performance Metrics</div>", unsafe_allow_html=True)

    m1, m2 = st.columns(2)

    with m1:
        bm = {"AUC-ROC":D["auc"],"AUC-PR":D["aupr"],
              "Accuracy":D["acc"],"F1":D["f1"],
              "Precision":D["prec"],"Recall":D["rec"]}
        fig12 = go.Figure(go.Bar(
            x=list(bm.keys()), y=[v*100 for v in bm.values()],
            marker=dict(
                color=[v*100 for v in bm.values()],
                colorscale=[[0,"#1a2d40"],[1,K["a1"]]],
                showscale=False, line=dict(width=0),
            ),
            text=[f"{v*100:.1f}%" for v in bm.values()],
            textposition="outside", textfont=dict(size=11, color=K["text"]),
        ))
        fig12.update_layout(
            title=dict(text="Bot Detection", font=dict(size=13, color=K["text"]), x=0),
            yaxis=dict(range=[0,108], **AXIS()),
            xaxis=AXIS(),
            **BL(300),
        )
        st.plotly_chart(fig12, use_container_width=True)

    with m2:
        em = {"Accuracy":D["et_acc"],"F1":D["et_f1"],
              "Precision":0.7891,"Recall":0.7834}
        fig13 = go.Figure(go.Bar(
            x=list(em.keys()), y=[v*100 for v in em.values()],
            marker=dict(
                color=[v*100 for v in em.values()],
                colorscale=[[0,"#1a1a2d"],[1,K["a3"]]],
                showscale=False, line=dict(width=0),
            ),
            text=[f"{v*100:.1f}%" for v in em.values()],
            textposition="outside", textfont=dict(size=11, color=K["text"]),
        ))
        fig13.update_layout(
            title=dict(text="Edit Type Classifier", font=dict(size=13, color=K["text"]), x=0),
            yaxis=dict(range=[0,108], **AXIS()),
            xaxis=AXIS(),
            **BL(300),
        )
        st.plotly_chart(fig13, use_container_width=True)

    m3, m4 = st.columns(2)

    with m3:
        fig14 = go.Figure(go.Bar(
            x=["Minor/Cleanup","Content Edit","Revert","Vandalism"],
            y=[41200, 28400, 19800, 10600],
            marker=dict(color=PAL[:4], line=dict(width=0)),
            text=["41,200","28,400","19,800","10,600"],
            textposition="outside", textfont=dict(size=11, color=K["text"]),
        ))
        fig14.update_layout(
            title=dict(text="Edit Type Predicted Distribution",
                       font=dict(size=13, color=K["text"]), x=0),
            yaxis=dict(range=[0,48000], **AXIS()),
            xaxis=AXIS(),
            **BL(300),
        )
        st.plotly_chart(fig14, use_container_width=True)

    with m4:
        mrl = ["AUC-ROC","F1","Accuracy","Precision","Recall"]
        fig15 = go.Figure()
        for nm, vals, clr in [
            ("Bot Detection",[.9847,.9388,.9421,.9445,.9421,.9847], K["a1"]),
            ("Edit Type",    [.78,  .7712,.7834,.7891,.7834,.78],   K["a3"]),
        ]:
            fig15.add_trace(go.Scatterpolar(
                r=vals, theta=mrl+[mrl[0]],
                fill="toself", name=nm,
                line=dict(color=clr, width=2), opacity=0.72,
                hovertemplate="<b>%{theta}</b>: %{r:.3f}<extra></extra>",
            ))
        fig15.update_layout(
            title=dict(text="Model Comparison Radar",
                       font=dict(size=13, color=K["text"]), x=0),
            polar=dict(
                bgcolor=K["pbg"],
                angularaxis=dict(tickcolor=K["muted"], gridcolor=K["grid"],
                                 tickfont=dict(size=10, color=K["muted"])),
                radialaxis=dict(range=[0,1], gridcolor=K["grid"],
                                tickfont=dict(size=8, color=K["muted"])),
            ),
            legend=LEG(x=1.1, y=0.5),
            **BL(300),
        )
        st.plotly_chart(fig15, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 6  PIPELINE
# ═══════════════════════════════════════════════════════════════════════════
with tabs[5]:
    st.markdown("<div class='sec'>Big Data Pipeline Architecture</div>",
                unsafe_allow_html=True)

    st.markdown(f"""
<div class="insight">
  <div class="tag">DATA ENGINEERING STACK</div>
  <b>Ingestion:</b> Wikimedia XML dumps + Wikipedia REST API (11.1M records)<br>
  <b>Processing:</b> Apache Spark 3.5.0 distributed ETL on Docker containers<br>
  <b>Storage:</b> Medallion architecture (Bronze → Silver → Gold) using Parquet columnar format<br>
  <b>ML Training:</b> PySpark MLlib with stratified sampling and hyperparameter tuning<br>
  <b>Infrastructure:</b> Docker Compose orchestrating Spark master/worker + WSL2
</div>""", unsafe_allow_html=True)

    pa, arr1, pb, arr2, pc, arr3, pd_ = st.columns([5,1,5,1,5,1,5])
    NODES = [
        (pa, "🥉", "Bronze Layer", K["a2"],
         ["Wikimedia XML dumps", "Wikipedia API data", "294 JSON files (11.1M records)",
          "Raw unprocessed format", "Schema: nested JSON"]),
        (pb, "🥈", "Silver Layer", K["a5"],
         ["Cleaned & validated", "17-field explicit schema",
          "Null-safe transformations", "Timestamp normalization", "Format: Parquet columnar"]),
        (pc, "🥇", "Gold Layer", K["a4"],
         ["28 engineered features", "User & page aggregates",
          "Edit velocity metrics", "Revert detection flags", "ML-ready feature sets"]),
        (pd_,"🤖", "ML Models", K["a3"],
         ["Random Forest (Bot: 98.5% AUC)", "Logistic Regression (Edit Type)",
          "K-Means (5 user clusters)", "Stratified sampling", "Saved PipelineModels"]),
    ]
    
    for col, icon, title, color, items in NODES:
        with col:
            rows = "".join(
                f"<div style='padding:.12rem 0;border-bottom:1px solid {K['border']};"
                f"font-size:.75rem;color:{K['text']}'>{i}</div>"
                for i in items
            )
            st.markdown(f"""
<div class="glass" style="text-align:center">
  <div style="font-size:1.7rem;margin-bottom:.25rem">{icon}</div>
  <div style="font-family:'JetBrains Mono',monospace;font-size:.62rem;
    color:{color};text-transform:uppercase;letter-spacing:.1em;
    margin-bottom:.55rem;font-weight:500">{title}</div>
  {rows}
</div>""", unsafe_allow_html=True)

    for col, arrow in [(arr1,"→"),(arr2,"→"),(arr3,"→")]:
        with col:
            st.markdown(
                f"<div style='text-align:center;font-size:1.9rem;"
                f"color:{K['muted']};padding-top:2.8rem'>{arrow}</div>",
                unsafe_allow_html=True)

    st.markdown("<div class='sec'>Technology Stack & Tools</div>", unsafe_allow_html=True)
    tc1,tc2,tc3,tc4 = st.columns(4)
    TECH = [
        (tc1,"⚙️","Apache Spark","v3.5.0",
         "Distributed ETL\n11.1M records processed\nDocker containerized", K["a1"]),
        (tc2,"🔬","PySpark MLlib","RF · LR · KMeans",
         "3 trained models\nStratified sampling\nPipelineModel API",  K["a3"]),
        (tc3,"🐳","Docker","Compose",
         "spark-master\nspark-worker\nVolume orchestration",                 K["a5"]),
        (tc4,"📊","Streamlit","+ Plotly 5",
         "Interactive dashboard\nGlassmorphism UI\nReal-time metrics",  K["a4"]),
    ]
    for col,icon,name,ver,desc,color in TECH:
        with col:
            st.markdown(f"""
<div class="glass">
  <div style="font-size:1.65rem;margin-bottom:.3rem">{icon}</div>
  <div style="font-weight:700;font-size:.95rem;color:{K['text']}">{name}</div>
  <div style="font-family:'JetBrains Mono',monospace;font-size:.62rem;
    color:{color};margin-bottom:.45rem">{ver}</div>
  <div style="font-size:.76rem;color:{K['muted']};
    line-height:1.78;white-space:pre-line">{desc}</div>
</div>""", unsafe_allow_html=True)

    st.markdown("<div class='sec'>Pipeline Execution Steps</div>", unsafe_allow_html=True)
    
    steps = [
        ("1. Data Ingestion", "Downloaded 3 Wikimedia XML dumps (1.3 GB compressed) + Wikipedia API batches", K["a1"]),
        ("2. Bronze Parsing", "Stream-parsed XML with bz2 decompression, extracted 17 metadata fields per revision", K["a2"]),
        ("3. Silver ETL", "Spark RDDs → DataFrames, null filtering, deduplication, timestamp normalization", K["a5"]),
        ("4. Feature Engineering", "Window functions for user/page aggregates, velocity metrics, 28 total features", K["a4"]),
        ("5. ML Training", "Random Forest (100 trees), Logistic Regression (multinomial), K-Means (k=5)", K["a3"]),
        ("6. Model Evaluation", "Cross-validation, stratified splits, AUC-ROC/F1/Silhouette metrics", K["a1"]),
    ]
    
    for step, desc, color in steps:
        st.markdown(f"""
<div class="glass" style="margin:.5rem 0">
  <div style="font-family:'JetBrains Mono',monospace;font-size:.65rem;
    color:{color};text-transform:uppercase;letter-spacing:.08em;
    margin-bottom:.25rem;font-weight:600">{step}</div>
  <div style="font-size:.82rem;color:{K['text']};line-height:1.65">{desc}</div>
</div>""", unsafe_allow_html=True)

    st.markdown("<div class='sec'>Pipeline Execution Results</div>", unsafe_allow_html=True)
    r1,r2,r3,r4,r5,r6 = st.columns(6)
    for col,lbl,val,color in [
        (r1,"Bronze Files","294",     K["a2"]),
        (r2,"Records","11.1M",K["a5"]),
        (r3,"Features","28",    K["a4"]),
        (r4,"Models","3",     K["a3"]),
        (r5,"Errors","0 ✓",    K["a1"]),
        (r6,"Bots Detected","1.73M",  K["a2"]),
    ]:
        with col:
            st.markdown(f"""
<div class="glass" style="text-align:center;padding:.9rem">
  <div style="font-size:1.55rem;font-weight:700;
    color:{color};line-height:1">{val}</div>
  <div style="font-size:.64rem;color:{K['muted']};margin-top:.28rem;
    font-family:'JetBrains Mono',monospace;text-transform:uppercase;
    letter-spacing:.06em">{lbl}</div>
</div>""", unsafe_allow_html=True)


# ── FOOTER ───────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:3rem;padding:1.4rem 0 .8rem;
  border-top:1px solid {K['border']};text-align:center">
  <div style="font-size:.74rem;color:{K['muted']};line-height:2.1">
    <span style="color:{K['text']};font-weight:700">Khouloud Ben Younes</span>
    &nbsp;·&nbsp;
    <span style="color:{K['text']};font-weight:700">Montaha Ghabri</span>
    &nbsp;&nbsp;|&nbsp;&nbsp;
    <span style="color:{K['a1']};font-family:'JetBrains Mono',monospace">
      Tunis Business School · 2025-2026
    </span>
    &nbsp;&nbsp;|&nbsp;&nbsp;
    Apache Spark · PySpark MLlib · Streamlit · 11.1M Wikipedia Edits
  </div>
</div>""", unsafe_allow_html=True)
