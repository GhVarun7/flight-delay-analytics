import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
import pickle
import json
import numpy as np
import os

# ── PAGE CONFIG ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Flight Delay Analytics",
    page_icon="✈️",
    layout="wide"
)

# ── DB CONNECTION ──────────────────────────────────────────────────────────
DB_HOST = "flight-delay-db.cxes4k8u8564.us-east-2.rds.amazonaws.com"
DB_NAME = "flightdb"
DB_USER = "postgres"
DB_PASS = "FlightDB2026!"
DB_PORT = 5432

@st.cache_data
def load_data():
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    df = pd.read_sql("""
        SELECT 
            f.*,
            a.airline_name,
            d.month_name,
            d.season,
            d.is_weekend,
            d.day_name,
            d.quarter
        FROM fact_flights f
        LEFT JOIN dim_airline a 
            ON f."AIRLINE" = a.airline_code
        LEFT JOIN dim_date d 
            ON (f."YEAR" * 10000 + f."MONTH" * 100 + f."DAY") = d.date_key
    """, engine)
    return df

# ── LOAD ───────────────────────────────────────────────────────────────────
with st.spinner("Loading data from RDS..."):
    df = load_data()

# ── HEADER ─────────────────────────────────────────────────────────────────
st.title("✈️ Flight Delay Analytics Dashboard")
st.caption("DAMG7370 Final Project | Varun Gehlot | Northeastern University Seattle")
st.markdown("---")

# ── KPI CARDS ──────────────────────────────────────────────────────────────
total_flights   = len(df)
delayed_flights = int(df["IS_DELAYED"].sum()) if "IS_DELAYED" in df.columns else int((df["ARRIVAL_DELAY"] > 15).sum())
delay_rate      = round(delayed_flights / total_flights * 100, 1)
avg_delay       = round(df[df["ARRIVAL_DELAY"] > 0]["ARRIVAL_DELAY"].mean(), 1)
cancelled       = int(df["CANCELLED"].sum())

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Flights",      f"{total_flights:,}")
k2.metric("Delayed Flights",    f"{delayed_flights:,}")
k3.metric("Delay Rate",         f"{delay_rate}%")
k4.metric("Avg Delay (mins)",   f"{avg_delay}")
k5.metric("Cancellations",      f"{cancelled:,}")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# SECTION 1 — FLIGHT DELAY TRENDS
# ══════════════════════════════════════════════════════════════════════════
st.header("Section 1: Flight Delay Trends")

col1, col2 = st.columns(2)

# Chart 1 — Delay Rate by Airline
with col1:
    st.subheader("Delay Rate by Airline")
    airline_stats = (
        df.groupby("airline_name")
        .agg(total=("ARRIVAL_DELAY", "count"),
             delayed=("IS_DELAYED", "sum"))
        .reset_index()
    )
    airline_stats["delay_rate"] = round(airline_stats["delayed"] / airline_stats["total"] * 100, 1)
    airline_stats = airline_stats.sort_values("delay_rate", ascending=True)

    fig1 = px.bar(
        airline_stats, x="delay_rate", y="airline_name",
        orientation="h",
        labels={"delay_rate": "Delay Rate (%)", "airline_name": "Airline"},
        color="delay_rate",
        color_continuous_scale="RdYlGn_r",
        text="delay_rate"
    )
    fig1.update_traces(texttemplate="%{text}%", textposition="outside")
    fig1.update_layout(coloraxis_showscale=False, height=420)
    st.plotly_chart(fig1, use_container_width=True)

# Chart 2 — Avg Arrival Delay by Airline
with col2:
    st.subheader("Avg Arrival Delay by Airline (mins)")
    avg_by_airline = (
        df[df["ARRIVAL_DELAY"] > 0]
        .groupby("airline_name")["ARRIVAL_DELAY"]
        .mean()
        .round(1)
        .reset_index()
        .sort_values("ARRIVAL_DELAY", ascending=True)
    )
    fig2 = px.bar(
        avg_by_airline, x="ARRIVAL_DELAY", y="airline_name",
        orientation="h",
        labels={"ARRIVAL_DELAY": "Avg Delay (mins)", "airline_name": "Airline"},
        color="ARRIVAL_DELAY",
        color_continuous_scale="OrRd",
        text="ARRIVAL_DELAY"
    )
    fig2.update_traces(texttemplate="%{text} min", textposition="outside")
    fig2.update_layout(coloraxis_showscale=False, height=420)
    st.plotly_chart(fig2, use_container_width=True)

col3, col4 = st.columns(2)

# Chart 3 — Delay Rate by Day of Week
with col3:
    st.subheader("Delay Rate by Day of Week")
    day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    day_stats = (
        df.groupby("day_name")
        .agg(total=("ARRIVAL_DELAY","count"), delayed=("IS_DELAYED","sum"))
        .reset_index()
    )
    day_stats["delay_rate"] = round(day_stats["delayed"] / day_stats["total"] * 100, 1)
    day_stats["day_name"] = pd.Categorical(day_stats["day_name"], categories=day_order, ordered=True)
    day_stats = day_stats.sort_values("day_name")

    fig3 = px.bar(
        day_stats, x="day_name", y="delay_rate",
        labels={"day_name": "Day", "delay_rate": "Delay Rate (%)"},
        color="delay_rate",
        color_continuous_scale="Blues",
        text="delay_rate"
    )
    fig3.update_traces(texttemplate="%{text}%", textposition="outside")
    fig3.update_layout(coloraxis_showscale=False, height=380)
    st.plotly_chart(fig3, use_container_width=True)

# Chart 4 — Delay Rate by Hour of Day
with col4:
    st.subheader("Delay Rate by Departure Hour")

    # Extract hour from SCHEDULED_DEPARTURE (format: HHMM integer e.g. 900 = 9:00am)
    df["DEP_HOUR"] = (df["SCHEDULED_DEPARTURE"] // 100).astype(int)
    df["DEP_HOUR"] = df["DEP_HOUR"].clip(0, 23)

    hour_stats = (
        df.groupby("DEP_HOUR")
        .agg(total=("ARRIVAL_DELAY","count"), delayed=("IS_DELAYED","sum"))
        .reset_index()
    )
    hour_stats = hour_stats[hour_stats["total"] >= 20]  # filter sparse hours
    hour_stats["delay_rate"] = round(hour_stats["delayed"] / hour_stats["total"] * 100, 1)
    hour_stats["hour_label"] = hour_stats["DEP_HOUR"].apply(
        lambda h: f"{h}am" if h < 12 else ("12pm" if h == 12 else f"{h-12}pm")
    )

    fig4 = px.line(
        hour_stats, x="hour_label", y="delay_rate",
        labels={"hour_label": "Departure Hour", "delay_rate": "Delay Rate (%)"},
        markers=True,
        color_discrete_sequence=["#0D9488"]
    )
    fig4.update_traces(line_width=2.5, marker_size=7)
    fig4.update_layout(height=380)
    st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# SECTION 2 — OPERATIONAL ANALYSIS
# ══════════════════════════════════════════════════════════════════════════
st.header("Section 2: Operational Analysis")

col5, col6 = st.columns(2)

# Chart 5 — Top 10 Most Delayed Origin Airports
with col5:
    st.subheader("Top 10 Most Delayed Origin Airports")
    origin_stats = (
        df.groupby("ORIGIN_AIRPORT")
        .agg(total=("ARRIVAL_DELAY","count"), delayed=("IS_DELAYED","sum"))
        .reset_index()
    )
    origin_stats["delay_rate"] = round(origin_stats["delayed"] / origin_stats["total"] * 100, 1)
    top_origins = origin_stats.nlargest(10, "delay_rate")

    fig5 = px.bar(
        top_origins, x="delay_rate", y="ORIGIN_AIRPORT",
        orientation="h",
        labels={"delay_rate": "Delay Rate (%)", "ORIGIN_AIRPORT": "Airport"},
        color="delay_rate",
        color_continuous_scale="Reds",
        text="delay_rate"
    )
    fig5.update_traces(texttemplate="%{text}%", textposition="outside")
    fig5.update_layout(coloraxis_showscale=False, height=400)
    st.plotly_chart(fig5, use_container_width=True)

# Chart 6 — Delay Causes Breakdown
with col6:
    st.subheader("Delay Causes Breakdown (Total Minutes)")
    delay_causes = {
        "Airline Delay":        df["AIRLINE_DELAY"].sum(),
        "Late Aircraft":        df["LATE_AIRCRAFT_DELAY"].sum(),
        "Air System Delay":     df["AIR_SYSTEM_DELAY"].sum(),
        "Weather Delay":        df["WEATHER_DELAY"].sum(),
        "Security Delay":       df["SECURITY_DELAY"].sum(),
    }
    causes_df = pd.DataFrame(list(delay_causes.items()), columns=["Cause","Total Minutes"])
    causes_df = causes_df.sort_values("Total Minutes", ascending=False)

    fig6 = px.bar(
        causes_df, x="Cause", y="Total Minutes",
        color="Cause",
        color_discrete_sequence=px.colors.qualitative.Pastel,
        text="Total Minutes"
    )
    fig6.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig6.update_layout(showlegend=False, height=400)
    st.plotly_chart(fig6, use_container_width=True)

col7, col8 = st.columns(2)

# Chart 7 — Top 10 Worst Routes
with col7:
    st.subheader("Top 10 Worst Routes by Avg Delay")
    df["route"] = df["ORIGIN_AIRPORT"] + " → " + df["DESTINATION_AIRPORT"]
    route_stats = (
        df[df["ARRIVAL_DELAY"] > 0]
        .groupby("route")["ARRIVAL_DELAY"]
        .agg(avg_delay="mean", flights="count")
        .reset_index()
    )
    # Only routes with at least 5 flights
    route_stats = route_stats[route_stats["flights"] >= 5]
    route_stats["avg_delay"] = route_stats["avg_delay"].round(1)
    top_routes = route_stats.nlargest(10, "avg_delay")

    fig7 = px.bar(
        top_routes, x="avg_delay", y="route",
        orientation="h",
        labels={"avg_delay": "Avg Delay (mins)", "route": "Route"},
        color="avg_delay",
        color_continuous_scale="OrRd",
        text="avg_delay"
    )
    fig7.update_traces(texttemplate="%{text} min", textposition="outside")
    fig7.update_layout(coloraxis_showscale=False, height=420)
    st.plotly_chart(fig7, use_container_width=True)

# Chart 8 — Weekend vs Weekday Delay Rate
with col8:
    st.subheader("Weekend vs Weekday Delay Rate")
    wk_stats = (
        df.groupby("is_weekend")
        .agg(total=("ARRIVAL_DELAY","count"), delayed=("IS_DELAYED","sum"))
        .reset_index()
    )
    wk_stats["label"] = wk_stats["is_weekend"].map({True: "Weekend", False: "Weekday"})
    wk_stats["delay_rate"] = round(wk_stats["delayed"] / wk_stats["total"] * 100, 1)

    fig8 = px.bar(
        wk_stats, x="label", y="delay_rate",
        labels={"label": "", "delay_rate": "Delay Rate (%)"},
        color="label",
        color_discrete_map={"Weekend": "#FF6B6B", "Weekday": "#4ECDC4"},
        text="delay_rate"
    )
    fig8.update_traces(texttemplate="%{text}%", textposition="outside")
    fig8.update_layout(showlegend=False, height=420)
    st.plotly_chart(fig8, use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════
# SECTION 3 — WHAT-IF DELAY PREDICTOR
# ══════════════════════════════════════════════════════════════════════════
st.header("Section 3: What-If Delay Predictor")
st.write("Select flight parameters below and the ML model will predict whether your flight is likely to be delayed.")

# ── Load model & encoders ──────────────────────────────────────────────────
MODEL_PATH    = r"C:\Users\gehlo\OneDrive\Desktop\DAMG Project\flight_delay_model.pkl"
ENCODERS_PATH = r"C:\Users\gehlo\OneDrive\Desktop\DAMG Project\encoders.pkl"
METRICS_PATH  = r"C:\Users\gehlo\OneDrive\Desktop\DAMG Project\model_metrics.json"

@st.cache_resource
def load_model():
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    with open(ENCODERS_PATH, "rb") as f:
        encoders = pickle.load(f)
    return model, encoders

@st.cache_data
def load_metrics():
    with open(METRICS_PATH, "r") as f:
        return json.load(f)

model, encoders = load_model()
metrics = load_metrics()

# ── Model Performance Cards ────────────────────────────────────────────────
st.subheader("Model Performance (Random Forest 97k flights)")
m1, m2, m3, m4 = st.columns(4)
m1.metric("Accuracy",  f"{metrics['accuracy']}%")
m2.metric("Precision", f"{metrics['precision']}%")
m3.metric("Recall",    f"{metrics['recall']}%")
m4.metric("F1 Score",  f"{metrics['f1']}%")

st.markdown("---")

# ── Feature Importance Chart ───────────────────────────────────────────────
st.subheader("What Drives Delays? Feature Importance")
fi_df = pd.DataFrame(metrics["feature_importance"]).sort_values("Importance", ascending=True)
fi_df["Feature"] = fi_df["Feature"].replace({
    "DEPARTURE_DELAY": "Departure Delay",
    "DAY_OF_WEEK":     "Day of Week",
    "AIR_TIME":        "Air Time",
    "DISTANCE":        "Distance",
    "ORIGIN_ENC":      "Origin Airport",
    "AIRLINE_ENC":     "Airline",
    "DEST_ENC":        "Destination Airport",
    "MONTH":           "Month"
})
fig_fi = px.bar(
    fi_df, x="Importance", y="Feature",
    orientation="h",
    color="Importance",
    color_continuous_scale="Blues",
    text=fi_df["Importance"].apply(lambda x: f"{x*100:.1f}%")
)
fig_fi.update_traces(textposition="outside")
fig_fi.update_layout(coloraxis_showscale=False, height=350)
st.plotly_chart(fig_fi, use_container_width=True)

st.markdown("---")

# ── Scenario Predictor ─────────────────────────────────────────────────────
st.subheader("Run a Scenario Will Your Flight Be Delayed?")

known_airlines = sorted(encoders["airline"].classes_.tolist())
known_origins  = sorted(encoders["origin"].classes_.tolist())
known_dests    = sorted(encoders["dest"].classes_.tolist())

col_a, col_b, col_c = st.columns(3)
with col_a:
    airline     = st.selectbox("Airline",             known_airlines, index=known_airlines.index("WN") if "WN" in known_airlines else 0)
    origin      = st.selectbox("Origin Airport",      known_origins,  index=known_origins.index("ATL") if "ATL" in known_origins else 0)
    destination = st.selectbox("Destination Airport", known_dests,    index=known_dests.index("LAX") if "LAX" in known_dests else 0)

with col_b:
    day_of_week    = st.selectbox("Day of Week", [1,2,3,4,5,6,7],
                                  format_func=lambda x: ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"][x-1])
    month          = st.selectbox("Month", list(range(1,13)),
                                  format_func=lambda x: ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][x-1])

with col_c:
    distance       = st.slider("Distance (miles)",    50,  3000, 800)
    departure_delay = st.slider("Departure Delay (mins)", -30, 300, 0,
                                help="0 = on time, negative = early, positive = already delayed")
    air_time       = st.slider("Air Time (mins)",     20,  600, 150)

st.markdown("")

# ── Predict ────────────────────────────────────────────────────────────────
if st.button("Predict Delay", use_container_width=True):

    # Encode inputs — handle unseen labels gracefully
    def safe_encode(encoder, value):
        if value in encoder.classes_:
            return encoder.transform([value])[0]
        else:
            return 0  # fallback for unknown

    airline_enc = safe_encode(encoders["airline"], airline)
    origin_enc  = safe_encode(encoders["origin"],  origin)
    dest_enc    = safe_encode(encoders["dest"],     destination)

    input_data = pd.DataFrame([{
        "AIRLINE_ENC":     airline_enc,
        "ORIGIN_ENC":      origin_enc,
        "DEST_ENC":        dest_enc,
        "DAY_OF_WEEK":     day_of_week,
        "MONTH":           month,
        "DISTANCE":        distance,
        "DEPARTURE_DELAY": departure_delay,
        "AIR_TIME":        air_time
    }])

    prediction   = model.predict(input_data)[0]
    probability  = model.predict_proba(input_data)[0]
    delay_prob   = round(probability[1] * 100, 1)
    ontime_prob  = round(probability[0] * 100, 1)

    st.markdown("---")

    if prediction == 1:
        st.error(f"### 🔴 Likely DELAYED — {delay_prob}% chance of delay")
    else:
        st.success(f"### 🟢 Likely ON TIME — {ontime_prob}% chance of on-time arrival")

    # Probability gauge
    gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=delay_prob,
        title={"text": "Delay Probability (%)"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar":  {"color": "#FF4B4B" if prediction == 1 else "#21BA45"},
            "steps": [
                {"range": [0,  33], "color": "#d4edda"},
                {"range": [33, 66], "color": "#fff3cd"},
                {"range": [66,100], "color": "#f8d7da"},
            ],
            "threshold": {
                "line": {"color": "black", "width": 4},
                "thickness": 0.75,
                "value": 50
            }
        }
    ))
    gauge.update_layout(height=300)
    st.plotly_chart(gauge, use_container_width=True)

    # Scenario summary
    day_name   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"][day_of_week-1]
    month_name = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month-1]
    st.info(f"**Scenario:** {airline} flight | {origin} → {destination} | {day_name}, {month_name} | "
            f"{distance} miles | Dep. delay: {departure_delay} mins | Air time: {air_time} mins")

st.markdown("---")

# ── FOOTER ─────────────────────────────────────────────────────────────────
st.caption("Data source: BTS Flight On-Time Performance Data | Pipeline: Kafka → S3 → RDS PostgreSQL | DAMG7370 Spring 2026")