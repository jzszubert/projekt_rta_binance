import streamlit as st
import pandas as pd
import time
import json
import sqlite3
import plotly.express as px
from kafka import KafkaConsumer

st.set_page_config(
    page_title="Bitcoin analysis dashboard",
    layout="wide",
)
st.title("Real-Time Bitcoin dashboard")
st.write("Refresh every 60 sec, click to refresh manually")

if st.button("Manual refresh"):
    st.rerun()



option = st.selectbox(
    "Select window length:", 
    ("1 min", "5 min")
)

@st.cache_data(ttl = 2)
def load_data(option):

    conn = sqlite3.connect("rta.db")
    query = f"""
    SELECT  *
    FROM vwap_history
    WHERE window_len = '{option}'
    """
    df =pd.read_sql(query, conn)
    conn.close()
    return df
    
def load_alerts_from_kafka():
    alerts_list = []
    try:
        consumer = KafkaConsumer(
            "alerts",
            bootstrap_servers="broker:9092",
            auto_offset_reset="earliest", 
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=300  
        )
        for msg in consumer:
            alerts_list.append(msg.value)
        consumer.close()
    except Exception as e:
        return pd.DataFrame([{"ERROR": f"Kafka Connection Error: {str(e)}"}])
        
    if alerts_list:
        return pd.DataFrame(alerts_list)
    return pd.DataFrame()

df = load_data(option)
df_alerts = load_alerts_from_kafka()

if not df_alerts.empty and "ERROR" not in df_alerts.columns:
    latest_alert = df_alerts.iloc[-1]
    alert_type = latest_alert.get('alert_type', 'UNKNOWN')
    asset = latest_alert.get('symbol', 'BTCUSDT')
    
    if alert_type == 'VOLUME_SPIKE':
        alert_msg = f" Spark detected VOLUME SPIKE ! Ratio: {latest_alert.get('ratio')}x"
        st.error(alert_msg)
        st.toast(alert_msg)
        
    elif alert_type == 'ANOMALY':
        alert_msg = f" ML Model detected PRICE/VOLUME ANOMALY on {asset}! Score: {latest_alert.get('score')}"
        st.error(alert_msg)
        st.toast(alert_msg)

if not df.empty:
    df['window_start'] = pd.to_datetime(df['window_start'])
    df['vwap'] = pd.to_numeric(df['vwap'], errors='coerce')
    df['total_volume'] = pd.to_numeric(df['total_volume'], errors='coerce')

    df = df.sort_values(by='window_start', ascending=True)

    df = df.drop_duplicates(subset=['window_start', 'window_len'], keep='last')

    st.subheader("Bitcoin Price Trend (VWAP)")
    fig_price = px.line(
        df, 
        x='window_start', 
        y='vwap', 
        labels={'window_start': 'Time', 'vwap': 'Price (USD)'}
    )
    fig_price.update_yaxes(autorange=True, fixedrange=False)
    st.plotly_chart(fig_price, use_container_width=True)

    st.subheader("Total Volume")
    df_volume = df.set_index('window_start')[['total_volume']]
    st.bar_chart(df_volume, use_container_width=True)

    st.subheader("Recent Bitcoin data")
    
    df_display = df.copy()
    df_display['window_start'] = df_display['window_start'].dt.strftime('%Y-%m-%d %H:%M')
    df_display['vwap'] = df_display['vwap'].map('{:,.2f}'.format)
    df_display['total_volume'] = df_display['total_volume'].map('{:,.4f}'.format)
    
    st.dataframe(df_display.iloc[::-1].head(10), use_container_width=True, hide_index=True)

else:
    st.info(f"No data available yet for {option}. Make sure your streaming pipeline is running.")


if not df_alerts.empty:
    if "ERROR" in df_alerts.columns:
        st.error(df_alerts["ERROR"].iloc[0])
    else:
        if 'detected_at' in df_alerts.columns:
            df_alerts['Time'] = pd.to_datetime(df_alerts['detected_at'], errors='coerce').dt.strftime('%H:%M:%S')
        elif 'col3' in df_alerts.columns:
            df_alerts['Time'] = pd.to_datetime(df_alerts['col3'], errors='coerce').dt.strftime('%H:%M:%S')
        else:
            df_alerts['Time'] = "N/A"

        if 'score' in df_alerts.columns and 'zscore' in df_alerts.columns:
            df_alerts['Metric Score'] = df_alerts['score'].combine_first(df_alerts['zscore'])
        elif 'score' in df_alerts.columns:
            df_alerts['Metric Score'] = df_alerts['score']
        elif 'zscore' in df_alerts.columns:
            df_alerts['Metric Score'] = df_alerts['zscore']
        else:
            df_alerts['Metric Score'] = 0.0

        def build_details(row):
            if row.get('alert_type') == 'VOLUME_SPIKE':
                return f"Vol ratio: {row.get('ratio', 'N/A')}x (Max: {row.get('max_volume', 'N/A')})"
            elif row.get('alert_type') == 'ANOMALY':
                return f"Price: ${row.get('price', 'N/A')} | Vol: {row.get('volume', 'N/A')}"
            return "Suspicious market activity"

        df_alerts['Details'] = df_alerts.apply(build_details, axis=1)

        for col in [ 'alert_type']:
            if col not in df_alerts.columns:
                df_alerts[col] = "N/A"

        df_alerts_display = df_alerts[['Time', 'alert_type', 'Metric Score', 'Details']].copy()
        df_alerts_display.columns = ['Alert Time', 'Trigger Type', 'Significance Score', 'Context / Details']

        def style_rows(row):
            if row['Trigger Type'] == 'VOLUME_SPIKE':
                return ['background-color: #fff3cd; color: #856404; font-weight: bold;'] * len(row)
            return ['background-color: #fce8e6; color: #a51d24; font-weight: bold;'] * len(row)

        st.dataframe(
            df_alerts_display.iloc[::-1].head(10).style.apply(style_rows, axis=1),
            use_container_width=True,
            hide_index=True
        )
else:
    st.success("✅ No anomalies detected in Kafka topic 'alerts'")
    
time.sleep(10)
st.rerun()