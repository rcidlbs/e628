# ═══════════════════════════════════════════════════════════════════════════════
#  Westminster Accounts — Interactive Dash Dashboard
#  Sky News Political Data Explorer
#  Run: python app.py  →  open http://127.0.0.1:8050
# ═══════════════════════════════════════════════════════════════════════════════

import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
import traceback
from dash import dcc, html, dash_table, Input, Output, State, callback_context
import dash_bootstrap_components as dbc

# ── 0. CONFIG ────────────────────────────────────────────────────────────────
DB_PATH = r"C:\e628\data\sky-westminster-files.db"

# Party colours (from DB background hex field; overrides below for readability)
# ── Inline logo data URIs (no external URLs needed) ──
PORTCULLIS_URI = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAxMDAgMTIwIiB3aWR0aD0iNDAiIGhlaWdodD0iNDgiPgogIDxwYXRoIGQ9Ik0xNSwzMCBMMjAsMTAgTDMwLDIyIEw1MCw4IEw3MCwyMiBMODAsMTAgTDg1LDMwIFoiIGZpbGw9IndoaXRlIi8+CiAgPHJlY3QgeD0iMTUiIHk9IjMwIiB3aWR0aD0iNzAiIGhlaWdodD0iNzAiIHJ4PSIyIiBmaWxsPSJub25lIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjUiLz4KICA8bGluZSB4MT0iMzUiIHkxPSIzMCIgeDI9IjM1IiB5Mj0iMTAwIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjQiLz4KICA8bGluZSB4MT0iNTAiIHkxPSIzMCIgeDI9IjUwIiB5Mj0iMTAwIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjQiLz4KICA8bGluZSB4MT0iNjUiIHkxPSIzMCIgeDI9IjY1IiB5Mj0iMTAwIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjQiLz4KICA8bGluZSB4MT0iMTUiIHkxPSI1MiIgeDI9Ijg1IiB5Mj0iNTIiIHN0cm9rZT0id2hpdGUiIHN0cm9rZS13aWR0aD0iNCIvPgogIDxsaW5lIHgxPSIxNSIgeTE9IjcyIiB4Mj0iODUiIHkyPSI3MiIgc3Ryb2tlPSJ3aGl0ZSIgc3Ryb2tlLXdpZHRoPSI0Ii8+CiAgPGxpbmUgeDE9IjM1IiB5MT0iMTAwIiB4Mj0iMzUiIHkyPSIxMTIiIHN0cm9rZT0id2hpdGUiIHN0cm9rZS13aWR0aD0iMyIvPgogIDxsaW5lIHgxPSI2NSIgeTE9IjEwMCIgeDI9IjY1IiB5Mj0iMTEyIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjMiLz4KICA8Y2lyY2xlIGN4PSIzNSIgY3k9IjExNSIgcj0iMyIgZmlsbD0id2hpdGUiLz4KICA8Y2lyY2xlIGN4PSI2NSIgY3k9IjExNSIgcj0iMyIgZmlsbD0id2hpdGUiLz4KPC9zdmc+"
SKYNEWS_URI    = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAxNjAgNDAiIHdpZHRoPSIxMzAiIGhlaWdodD0iMzIiPgogIDxyZWN0IHg9IjAiIHk9IjAiIHdpZHRoPSIxNjAiIGhlaWdodD0iNDAiIHJ4PSI0IiBmaWxsPSIjMDA3MkNFIi8+CiAgPHRleHQgeD0iMTIiIHk9IjI4IiBmb250LWZhbWlseT0iQXJpYWwgQmxhY2ssIEFyaWFsIiBmb250LXdlaWdodD0iOTAwIiBmb250LXNpemU9IjIyIiBmaWxsPSJ3aGl0ZSIgbGV0dGVyLXNwYWNpbmc9IjEiPlNLWSBORVdTPC90ZXh0Pgo8L3N2Zz4="

PARTY_COLOUR_OVERRIDES = {
    "Democratic Unionist Party": "#800000",
    "Speaker":                   "#888888",
    "Alliance":                  "#F6CB2F",
    "Alba Party":                "#0015FF",
    "Conservative":              "#0087DC",
    "Labour":                    "#E4003B",
    "Liberal Democrats":         "#FAA61A",
    "Scottish National Party":   "#FDF38E",
    "Green Party":               "#78B82A",
    "Sinn Féin":                 "#326760",
    "Plaid Cymru":               "#3F8428",
    "Independent":               "#AAAAAA",
    "Social Democratic & Labour Party": "#2AA82C",
}

# ── 1. DATA LOADING  (runs ONCE at startup — never inside callbacks) ──────────
def load_all_data():
    """Load and pre-process all required tables into memory."""
    try:
        conn = sqlite3.connect(DB_PATH)

        payments_raw   = pd.read_sql_query("SELECT * FROM payments",        conn)
        members        = pd.read_sql_query("SELECT * FROM members",         conn)
        parties        = pd.read_sql_query("SELECT * FROM parties",         conn)
        party_don_raw  = pd.read_sql_query("SELECT * FROM party_donations", conn)

        conn.close()
    except Exception as e:
        raise RuntimeError(f"Could not open database at '{DB_PATH}': {e}")

    # ── Party colour map — use hardcoded dict, DB hex values are unreliable ──
    colour_map = dict(PARTY_COLOUR_OVERRIDES)  # already has all 13 parties

    # ── Parse payments date → year (text like "Registered in November 2021") ──
    def extract_year(s):
        try:
            return int(str(s).strip().split()[-1])
        except Exception:
            return None

    payments_raw["year"] = payments_raw["date"].apply(extract_year)
    payments_raw = payments_raw.dropna(subset=["year"])
    payments_raw["year"] = payments_raw["year"].astype(int)

    # ── Join payments with members + parties ──
    # Rename members.id before merge to avoid collision with payments.id
    members_clean = members[["id", "name", "party_id", "status", "constituency"]].rename(
        columns={"id": "member_id_key", "name": "mp_name"}
    )
    parties_clean = parties[["id", "name"]].rename(
        columns={"id": "party_id_key", "name": "party_name"}
    )
    payments = (
        payments_raw
        .merge(members_clean, left_on="member_id", right_on="member_id_key", how="left")
        .drop(columns=["member_id_key"], errors="ignore")
        .merge(parties_clean, left_on="party_id", right_on="party_id_key", how="left")
        .drop(columns=["party_id_key"], errors="ignore")
    )

    # ── Parse party_donations date → year ──
    party_don_raw["year"] = pd.to_datetime(party_don_raw["date"], errors="coerce").dt.year
    party_don = (
        party_don_raw
        .merge(parties[["id", "name"]], left_on="party_id", right_on="id", how="left")
        .rename(columns={"name": "party_name"})
        .drop(columns=["id"], errors="ignore")
    )

    # ── Pre-compute: Top MPs ──
    top_mps = (
        payments.groupby(["member_id", "mp_name", "party_name"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "total_payments"})
        .sort_values("total_payments", ascending=False)
    )

    # ── Pre-compute: Donor type (single vs multi-party) ──
    entity_summary = (
        payments.groupby("entity")
        .agg(num_parties=("party_name", "nunique"),
             total_donated=("value", "sum"))
        .reset_index()
    )
    entity_summary["donor_type"] = entity_summary["num_parties"].apply(
        lambda x: "Single-party" if x == 1 else "Multi-party"
    )

    return {
        "payments":       payments,
        "members":        members,
        "parties":        parties,
        "party_don":      party_don,
        "top_mps":        top_mps,
        "entity_summary": entity_summary,
        "colour_map":     colour_map,
    }


DATA = load_all_data()

# Derived constants for controls
ALL_YEARS    = sorted(DATA["payments"]["year"].dropna().unique().tolist())
MIN_YEAR     = min(ALL_YEARS)
MAX_YEAR     = max(ALL_YEARS)
ALL_PARTIES  = sorted(DATA["parties"]["name"].dropna().unique().tolist())
DEFAULT_PARTIES = ["Conservative", "Labour", "Liberal Democrats",
                   "Scottish National Party", "Sinn Féin"]
ALL_MP_OPTIONS = [
    {"label": row["mp_name"], "value": row["member_id"]}
    for _, row in DATA["top_mps"].iterrows()
    if pd.notna(row["mp_name"])
]

# ── 2. HELPER: format £ ──────────────────────────────────────────────────────
def fmt_gbp(val):
    if val >= 1_000_000:
        return f"£{val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"£{val/1_000:.0f}K"
    return f"£{val:,.0f}"


# ── 3. LAYOUT COMPONENTS ─────────────────────────────────────────────────────

# ── Sidebar / global filter panel ──
def make_sidebar():
    return dbc.Card([
        dbc.CardHeader(html.H6("🎛  Global Filters", className="mb-0 text-uppercase fw-bold")),
        dbc.CardBody([

            html.Label("Year Range", className="fw-semibold small text-muted"),
            dcc.RangeSlider(
                id="year-slider",
                min=MIN_YEAR, max=MAX_YEAR,
                step=1,
                value=[2020, MAX_YEAR],
                marks={y: str(y) for y in ALL_YEARS},
                tooltip={"placement": "bottom", "always_visible": True},
                allowCross=False,
            ),
            html.Hr(),

            html.Label("Parties", className="fw-semibold small text-muted mt-2"),
            dbc.Button("Select All",   id="party-select-all",   size="sm",
                       color="outline-secondary", className="me-1 mb-2"),
            dbc.Button("Clear All",    id="party-clear-all",    size="sm",
                       color="outline-danger",    className="mb-2"),
            dcc.Checklist(
                id="party-checklist",
                options=[{"label": f"  {p}", "value": p} for p in ALL_PARTIES],
                value=DEFAULT_PARTIES,
                inputStyle={"marginRight": "6px"},
                labelStyle={"display": "block", "fontSize": "0.85rem"},
            ),
            html.Hr(),

            html.Label("Compare MPs", className="fw-semibold small text-muted mt-2"),
            dcc.Dropdown(
                id="mp-multiselect",
                options=ALL_MP_OPTIONS,
                value=[ALL_MP_OPTIONS[i]["value"] for i in range(min(5, len(ALL_MP_OPTIONS)))],
                multi=True,
                placeholder="Search and select MPs…",
                style={"fontSize": "0.85rem"},
            ),
        ])
    ], className="sticky-top", style={"top": "80px"})


# ── Tab 1: MP Analysis ──
tab1_content = dbc.Container([
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Top MPs by Total Payments Received"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Show Top N MPs", className="small text-muted"),
                            dcc.Slider(id="top-mps-slider", min=5, max=50, step=5, value=20,
                                marks={5:"5", 10:"10", 20:"20", 30:"30", 50:"50"},
                                tooltip={"placement":"bottom","always_visible":True}),
                        ], md=6),
                    ], className="mb-2"),
                    dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-top-mps", config={"displayModeBar": False})),
                ])
            ], className="mb-4"),
        ], md=8),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Payments by Category"),
                dbc.CardBody(dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-categories", config={"displayModeBar": False})))
            ], className="mb-4"),
        ], md=4),
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("MP Comparison — Selected MPs Side by Side"),
                dbc.CardBody(dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-mp-compare", config={"displayModeBar": False})))
            ], className="mb-4"),
        ], md=12),
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Detailed Payments Table"),
                dbc.CardBody([
                    dash_table.DataTable(
                        id="table-payments",
                        columns=[
                            {"name": "MP",         "id": "mp_name"},
                            {"name": "Party",      "id": "party_name"},
                            {"name": "Entity",     "id": "entity"},
                            {"name": "Category",   "id": "category_name"},
                            {"name": "Year",       "id": "year"},
                            {"name": "Value (£)",  "id": "value",
                             "type": "numeric",
                             "format": {"specifier": ",.0f"}},
                        ],
                        page_size=15,
                        sort_action="native",
                        filter_action="native",
                        style_table={"overflowX": "auto"},
                        style_cell={"fontSize": "0.82rem", "padding": "6px"},
                        style_header={"fontWeight": "bold", "backgroundColor": "#f8f9fa"},
                        style_data_conditional=[
                            {"if": {"row_index": "odd"}, "backgroundColor": "#fafafa"}
                        ],
                    )
                ])
            ])
        ])
    ])
], fluid=True, className="pt-3")


# ── Tab 2: Party Donations ──
tab2_content = dbc.Container([
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Party Donations by Year"),
                dbc.CardBody(dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-party-by-year", config={"displayModeBar": False})))
            ], className="mb-4"),
        ], md=7),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Share of Total Donations"),
                dbc.CardBody(dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-party-pie", config={"displayModeBar": False})))
            ], className="mb-4"),
        ], md=5),
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Cumulative Donations Over Time"),
                dbc.CardBody(dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-party-cumulative", config={"displayModeBar": False})))
            ], className="mb-4"),
        ], md=12),
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Top Individual Donors to Parties"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Min. Donation (£)", className="small text-muted"),
                            dcc.Slider(
                                id="min-donation-slider",
                                min=0, max=500000, step=10000, value=0,
                                marks={0: "£0", 100000: "£100K",
                                       250000: "£250K", 500000: "£500K"},
                                tooltip={"placement": "bottom", "always_visible": True},
                            ),
                        ], md=4),
                        dbc.Col([
                            html.Label("Show Top N Donors", className="small text-muted"),
                            dcc.Slider(
                                id="top-donors-slider",
                                min=5, max=30, step=5, value=15,
                                marks={5:"5", 10:"10", 15:"15", 20:"20", 30:"30"},
                                tooltip={"placement": "bottom", "always_visible": True},
                            ),
                        ], md=4),
                        dbc.Col([
                            html.Label("Nature of Donation", className="small text-muted"),
                            dcc.Dropdown(
                                id="nature-dropdown",
                                options=[{"label": "All", "value": "All"}] + [
                                    {"label": n, "value": n}
                                    for n in sorted(
                                        DATA["party_don"]["nature_of_donation"]
                                        .dropna().unique().tolist()
                                    ) if n
                                ],
                                value="All",
                                clearable=False,
                                style={"fontSize": "0.85rem"},
                            ),
                        ], md=6),
                    ], className="mb-3"),
                    dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-top-donors-party", config={"displayModeBar": False})),
                ])
            ])
        ])
    ])
], fluid=True, className="pt-3")


# ── Tab 3: Donor Intelligence ──
tab3_content = dbc.Container([
    # KPI Cards
    dbc.Row(id="kpi-cards", className="mb-4 g-3"),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Entity Loyalty — Total Donated vs. Number of Parties Funded"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Min. Total Donated (£)", className="small text-muted"),
                            dcc.Slider(
                                id="scatter-min-slider",
                                min=0, max=200000, step=5000, value=10000,
                                marks={0: "£0", 50000: "£50K",
                                       100000: "£100K", 200000: "£200K"},
                                tooltip={"placement": "bottom", "always_visible": True},
                            ),
                        ])
                    ], className="mb-2"),
                    dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-donor-scatter", config={"displayModeBar": False})),
                ])
            ], className="mb-4"),
        ], md=7),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Entities > Threshold % of Total Payments"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Threshold (%)", className="small text-muted"),
                            dcc.Slider(
                                id="pct-threshold-slider",
                                min=0.5, max=10, step=0.5, value=2,
                                marks={1: "1%", 3: "3%", 5: "5%", 10: "10%"},
                                tooltip={"placement": "bottom", "always_visible": True},
                            ),
                        ])
                    ], className="mb-2"),
                    dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-pct-entities", config={"displayModeBar": False})),
                ])
            ], className="mb-4"),
        ], md=5),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Top Entities — Party Distribution Sunburst"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Top N Entities", className="small text-muted"),
                            dcc.Slider(
                                id="sunburst-n-slider",
                                min=5, max=30, step=5, value=10,
                                marks={5: "5", 10: "10", 20: "20", 30: "30"},
                                tooltip={"placement": "bottom", "always_visible": True},
                            ),
                        ], md=6),
                    ], className="mb-2"),
                    dcc.Loading(type="circle", color="#6c757d", children=dcc.Graph(id="chart-sunburst", config={"displayModeBar": False})),
                ])
            ])
        ])
    ]),
], fluid=True, className="pt-3")


# ── 4. APP LAYOUT ─────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.LUX],
    title="Westminster Accounts",
    suppress_callback_exceptions=True,
)

app.layout = dbc.Container([

    # ── Navbar ──
    dbc.Navbar(
        dbc.Container([
            html.A(
                dbc.Row([
                    # Portcullis — inline SVG, no external URL needed
                    dbc.Col(html.Img(
                        src=PORTCULLIS_URI,
                        height="42px",
                        style={"marginRight": "8px"},
                    )),
                    # Sky News logo — inline SVG
                    dbc.Col(html.Img(
                        src=SKYNEWS_URI,
                        height="28px",
                        style={"marginRight": "12px"},
                    )),
                    dbc.Col(dbc.NavbarBrand([
                        html.Span("Westminster Accounts", className="fw-bold"),
                        html.Span(" — Political Data Explorer",
                                  className="fw-light text-white-50 ms-2 small"),
                    ])),
                ], align="center"),
                href="#", style={"textDecoration": "none"},
            ),
        ], fluid=True),
        color="dark", dark=True, className="mb-0",
        style={"position": "sticky", "top": 0, "zIndex": 1000},
    ),

    # ── Main layout: sidebar + tabs ──
    dbc.Row([
        dbc.Col(make_sidebar(), md=2, className="pt-3"),
        dbc.Col([
            dbc.Tabs([
                dbc.Tab(tab1_content, label="🏛 MP Analysis",      tab_id="tab-1"),
                dbc.Tab(tab2_content, label="🎗 Party Trends",      tab_id="tab-2"),
                dbc.Tab(tab3_content, label="🔍 Donor Intelligence", tab_id="tab-3"),
            ], id="main-tabs", active_tab="tab-1"),
        ], md=10),
    ], className="g-0"),

], fluid=True, className="px-0")


# ── 5. CALLBACKS ──────────────────────────────────────────────────────────────

# ── Select / Clear all parties ──
@app.callback(
    Output("party-checklist", "value"),
    Input("party-select-all", "n_clicks"),
    Input("party-clear-all",  "n_clicks"),
    State("party-checklist", "value"),
    prevent_initial_call=True,
)
def toggle_parties(sel, clr, current):
    ctx = callback_context.triggered_id
    if ctx == "party-select-all":
        return ALL_PARTIES
    if ctx == "party-clear-all":
        return []
    return current


# ── Helper: filter payments ──
def filter_payments(year_range, parties_selected):
    df = DATA["payments"].copy()
    df = df[df["year"].between(year_range[0], year_range[1])]
    if parties_selected:
        df = df[df["party_name"].isin(parties_selected)]
    return df


def filter_party_don(year_range, parties_selected):
    df = DATA["party_don"].copy()
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    df = df[df["year"].between(year_range[0], year_range[1])]
    if parties_selected:
        df = df[df["party_name"].isin(parties_selected)]
    return df


# ── TAB 1: Top MPs bar chart ──
@app.callback(
    Output("chart-top-mps", "figure"),
    Input("year-slider",      "value"),
    Input("party-checklist",  "value"),
    Input("top-mps-slider",   "value"),
)
def update_top_mps(year_range, parties_selected, top_n):
    top_n = top_n or 20
    try:
        df = filter_payments(year_range, parties_selected)
        agg = (
            df.groupby(["mp_name", "party_name"], as_index=False)["value"]
            .sum()
            .sort_values("value", ascending=False)
            .head(top_n)
        )
        agg["label"] = agg["value"].apply(fmt_gbp)
        fig = px.bar(
            agg, x="value", y="mp_name",
            color="party_name",
            color_discrete_map=DATA["colour_map"],
            orientation="h",
            text="label",
            labels={"value": "Total (£)", "mp_name": "", "party_name": "Party"},
            template="plotly_white",
        )
        fig.update_traces(textposition="inside", textfont_size=11, cliponaxis=False)
        fig.update_layout(
            yaxis={"autorange": "reversed"},
            showlegend=True,
            margin=dict(l=10, r=160, t=10, b=10),
            height=500,
            uniformtext_minsize=8,
            uniformtext_mode="hide",
            legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
        )
        return fig
    except Exception as e:
        print(f"\n❌ ERROR in update_top_mps:\n{traceback.format_exc()}")
        return go.Figure().update_layout(title=f"Error: {e}", template="plotly_white")


# ── TAB 1: Categories pie ──
@app.callback(
    Output("chart-categories", "figure"),
    Input("year-slider",     "value"),
    Input("party-checklist", "value"),
)
def update_categories(year_range, parties_selected):
    df = filter_payments(year_range, parties_selected)
    agg = df.groupby("category_name", as_index=False)["value"].sum()
    fig = px.pie(
        agg, values="value", names="category_name",
        hole=0.4,
        template="plotly_white",
        color_discrete_sequence=["#66c2a5","#fc8d62","#8da0cb","#e78ac3","#a6d854","#ffd92f"],
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=480, showlegend=False)
    return fig


# ── TAB 1: MP comparison grouped bar ──
@app.callback(
    Output("chart-mp-compare", "figure"),
    Input("year-slider",     "value"),
    Input("party-checklist", "value"),
    Input("mp-multiselect",  "value"),
)
def update_mp_compare(year_range, parties_selected, mp_ids):
    try:
        import traceback as tb
        # When specific MPs are selected, ignore party filter so they always appear
        df = DATA["payments"].copy()
        df = df[df["year"].between(year_range[0], year_range[1])]
        if mp_ids:
            df = df[df["member_id"].isin(mp_ids)]
        elif parties_selected:
            df = df[df["party_name"].isin(parties_selected)]
        agg = df.groupby(["mp_name", "category_name"], as_index=False)["value"].sum()
        if agg.empty:
            return go.Figure().update_layout(title="No data for selection", template="plotly_white")
        fig = px.bar(
            agg, x="mp_name", y="value",
            color="category_name",
            barmode="group",
            labels={"value": "Total (£)", "mp_name": "", "category_name": "Category"},
            template="plotly_white",
            color_discrete_sequence=["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b"],
        )
        fig.update_layout(
            margin=dict(l=10, r=10, t=10, b=10), height=380,
            legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
            xaxis_tickangle=-30,
        )
        return fig
    except Exception as e:
        import traceback as tb
        print(f"\n❌ update_mp_compare:\n{tb.format_exc()}")
        return go.Figure().update_layout(title=f"Error: {e}", template="plotly_white")


# ── TAB 1: Payments table ──
@app.callback(
    Output("table-payments", "data"),
    Input("year-slider",     "value"),
    Input("party-checklist", "value"),
    Input("mp-multiselect",  "value"),
)
def update_table(year_range, parties_selected, mp_ids):
    df = filter_payments(year_range, parties_selected)
    if mp_ids:
        df = df[df["member_id"].isin(mp_ids)]
    cols = ["mp_name", "party_name", "entity", "category_name", "year", "value"]
    return df[cols].sort_values("value", ascending=False).to_dict("records")


# ── TAB 2: Party donations grouped bar ──
@app.callback(
    Output("chart-party-by-year", "figure"),
    Input("year-slider",     "value"),
    Input("party-checklist", "value"),
)
def update_party_by_year(year_range, parties_selected):
    df = filter_party_don(year_range, parties_selected)
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    agg = df.groupby(["year", "party_name"], as_index=False)["value"].sum()
    agg["year"] = agg["year"].astype(int).astype(str)  # "2020" not "2020.0"
    if agg.empty:
        return go.Figure().update_layout(title="No data", template="plotly_white")
    fig = px.bar(
        agg, x="year", y="value",
        color="party_name",
        color_discrete_map=DATA["colour_map"],
        barmode="group",
        labels={"value": "Total Donations (£)", "year": "Year", "party_name": "Party"},
        template="plotly_white",
        text_auto=".2s",
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=10), height=380,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
    )
    return fig


# ── TAB 2: Party donations pie ──
@app.callback(
    Output("chart-party-pie", "figure"),
    Input("year-slider",     "value"),
    Input("party-checklist", "value"),
)
def update_party_pie(year_range, parties_selected):
    df = filter_party_don(year_range, parties_selected)
    agg = df.groupby("party_name", as_index=False)["value"].sum()
    fig = px.pie(
        agg, values="value", names="party_name",
        hole=0.4,
        color="party_name",
        color_discrete_map=DATA["colour_map"],
        template="plotly_white",
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=380, showlegend=False)
    return fig


# ── TAB 2: Cumulative donations line ──
@app.callback(
    Output("chart-party-cumulative", "figure"),
    Input("year-slider",     "value"),
    Input("party-checklist", "value"),
)
def update_party_cumulative(year_range, parties_selected):
    df = filter_party_don(year_range, parties_selected)
    df["month"] = pd.to_datetime(df["date"], errors="coerce").dt.to_period("M").astype(str)
    agg = df.groupby(["month", "party_name"], as_index=False)["value"].sum()
    agg = agg.sort_values("month")
    agg["cumulative"] = agg.groupby("party_name")["value"].cumsum()
    fig = px.area(
        agg, x="month", y="cumulative",
        color="party_name",
        color_discrete_map=DATA["colour_map"],
        labels={"cumulative": "Cumulative Donations (£)", "month": "", "party_name": "Party"},
        template="plotly_white",
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=10), height=320,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
        xaxis_tickangle=-30,
    )
    return fig


# ── TAB 2: Top donors to parties bar ──
@app.callback(
    Output("chart-top-donors-party", "figure"),
    Input("year-slider",        "value"),
    Input("party-checklist",    "value"),
    Input("min-donation-slider","value"),
    Input("nature-dropdown",    "value"),
    Input("top-donors-slider",  "value"),
)
def update_top_donors_party(year_range, parties_selected, min_don, nature, top_n):
    top_n = top_n or 15
    df = filter_party_don(year_range, parties_selected)
    if nature != "All":
        df = df[df["nature_of_donation"] == nature]
    agg = df.groupby(["entity", "party_name"], as_index=False)["value"].sum()
    agg = agg[agg["value"] >= min_don]
    top_entities = (
        agg.groupby("entity")["value"].sum()
        .sort_values(ascending=False).head(top_n).index
    )
    agg = agg[agg["entity"].isin(top_entities)]
    if agg.empty:
        return go.Figure().update_layout(title="No data for selection", template="plotly_white")
    fig = px.bar(
        agg, x="value", y="entity",
        color="party_name",
        color_discrete_map=DATA["colour_map"],
        orientation="h",
        labels={"value": "Total (£)", "entity": "", "party_name": "Party"},
        template="plotly_white",
        text_auto=".2s",
    )
    fig.update_layout(
        yaxis={"autorange": "reversed"},
        margin=dict(l=10, r=10, t=10, b=10), height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
    )
    return fig


# ── TAB 3: KPI Cards ──
@app.callback(
    Output("kpi-cards", "children"),
    Input("year-slider",     "value"),
    Input("party-checklist", "value"),
)
def update_kpis(year_range, parties_selected):
    df  = filter_payments(year_range, parties_selected)
    pd2 = filter_party_don(year_range, parties_selected)

    total_mp   = df["value"].sum()
    total_pd   = pd2["value"].sum()
    n_entities = df["entity"].nunique()

    es = (
        df.groupby("entity")
        .agg(num_parties=("party_name", "nunique"))
        .reset_index()
    )
    single_pct = (es["num_parties"] == 1).mean() * 100 if len(es) else 0

    def kpi_card(title, value, subtitle, color):
        return dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6(title, className="text-muted small text-uppercase mb-1"),
                html.H3(value, className=f"fw-bold text-{color}"),
                html.Small(subtitle, className="text-muted"),
            ])
        ], className="text-center border-0 shadow-sm"), md=3)

    return [
        kpi_card("Total MP Payments",      fmt_gbp(total_mp),  f"{year_range[0]}–{year_range[1]}", "dark"),
        kpi_card("Total Party Donations",  fmt_gbp(total_pd),  f"{year_range[0]}–{year_range[1]}", "primary"),
        kpi_card("Distinct Donor Entities", f"{n_entities:,}", "unique entities paying MPs", "secondary"),
        kpi_card("Single-Party Donors",    f"{single_pct:.1f}%", "of entities give to 1 party only", "success"),
    ]


# ── TAB 3: Donor scatter ──
@app.callback(
    Output("chart-donor-scatter", "figure"),
    Input("year-slider",      "value"),
    Input("party-checklist",  "value"),
    Input("scatter-min-slider","value"),
)
def update_donor_scatter(year_range, parties_selected, min_total):
    df = filter_payments(year_range, parties_selected)
    es = (
        df.groupby("entity")
        .agg(num_parties=("party_name", "nunique"),
             total_donated=("value", "sum"),
             num_mps=("member_id", "nunique"))
        .reset_index()
    )
    es["donor_type"] = es["num_parties"].apply(
        lambda x: "Single-party" if x == 1 else "Multi-party"
    )
    es = es[es["total_donated"] >= min_total]
    if es.empty:
        return go.Figure().update_layout(title="No data for selection", template="plotly_white")
    fig = px.scatter(
        es, x="total_donated", y="num_parties",
        size="num_mps", color="donor_type",
        hover_name="entity",
        hover_data={"total_donated": ":,.0f", "num_mps": True},
        color_discrete_map={"Single-party": "#2196F3", "Multi-party": "#FF5722"},
        labels={"total_donated": "Total Donated (£)", "num_parties": "No. of Parties Funded",
                "num_mps": "MPs Funded", "donor_type": "Donor Type"},
        template="plotly_white",
        log_x=True,
    )
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=400)
    return fig


# ── TAB 3: % threshold bar ──
@app.callback(
    Output("chart-pct-entities", "figure"),
    Input("year-slider",          "value"),
    Input("party-checklist",      "value"),
    Input("pct-threshold-slider", "value"),
)
def update_pct_entities(year_range, parties_selected, threshold):
    df = filter_payments(year_range, parties_selected)
    grand_total = df["value"].sum()
    if grand_total == 0:
        return go.Figure().update_layout(title="No data", template="plotly_white")
    agg = (
        df.groupby(["entity", "mp_name"], as_index=False)["value"].sum()
    )
    agg["pct"] = agg["value"] / grand_total * 100
    above = agg[agg["pct"] >= threshold].sort_values("pct", ascending=False)
    if above.empty:
        return go.Figure().update_layout(
            title=f"No entity exceeds {threshold}%", template="plotly_white"
        )
    fig = px.bar(
        above, x="pct", y="entity",
        color="mp_name",
        orientation="h",
        labels={"pct": "% of All Payments", "entity": "", "mp_name": "MP"},
        template="plotly_white",
        text_auto=".1f",
    )
    fig.update_layout(
        yaxis={"autorange": "reversed"},
        margin=dict(l=10, r=10, t=10, b=10), height=400,
        showlegend=True,
    )
    return fig


# ── TAB 3: Sunburst ──
@app.callback(
    Output("chart-sunburst", "figure"),
    Input("year-slider",       "value"),
    Input("party-checklist",   "value"),
    Input("sunburst-n-slider", "value"),
)
def update_sunburst(year_range, parties_selected, top_n):
    df = filter_payments(year_range, parties_selected)
    top_entities = (
        df.groupby("entity")["value"].sum()
        .sort_values(ascending=False).head(top_n).index
    )
    df = df[df["entity"].isin(top_entities)]
    agg = df.groupby(["entity", "party_name", "mp_name"], as_index=False)["value"].sum()
    if agg.empty:
        return go.Figure().update_layout(title="No data", template="plotly_white")
    fig = px.sunburst(
        agg, path=["entity", "party_name", "mp_name"], values="value",
        color="party_name",
        color_discrete_map=DATA["colour_map"],
        template="plotly_white",
    )
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=550)
    return fig


# ── 6. DEBUG ROUTE ───────────────────────────────────────────────────────────
from flask import jsonify

@app.server.route("/debug")
def debug():
    import traceback
    results = {}
    try:
        df = DATA["payments"]
        results["payments_cols"] = list(df.columns)
        results["payments_rows"] = len(df)
        results["has_mp_name"] = "mp_name" in df.columns
        results["has_party_name"] = "party_name" in df.columns
        results["has_member_id"] = "member_id" in df.columns
        results["year_values"] = sorted(df["year"].dropna().unique().tolist())
        results["party_values"] = sorted(df["party_name"].dropna().unique().tolist())
    except Exception as e:
        results["payments_error"] = traceback.format_exc()

    try:
        DEFAULT = ["Conservative", "Labour", "Liberal Democrats", "Scottish National Party", "Sinn Féin"]
        df2 = DATA["payments"].copy()
        df2 = df2[df2["year"].between(2019, 2022)]
        df2 = df2[df2["party_name"].isin(DEFAULT)]
        agg = df2.groupby(["mp_name", "party_name"], as_index=False)["value"].sum().sort_values("value", ascending=False).head(5)
        results["top_mps_test"] = agg[["mp_name","party_name","value"]].to_dict("records")
    except Exception as e:
        results["top_mps_error"] = traceback.format_exc()

    return jsonify(results)


# ── 6. RUN ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    app.run(debug=True, port=8050, dev_tools_props_check=False)
