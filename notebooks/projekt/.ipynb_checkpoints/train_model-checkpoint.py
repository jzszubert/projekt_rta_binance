import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

DB_PATH = os.getenv("DB_PATH", "rta.db")
MODEL_PATH = os.getenv("MODEL_PATH", "model.pkl")
CONTAMINATION = float(os.getenv("CONTAMINATION", "0.02"))
FEATURES = ["price", "volume"]


def load_training_data(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT vwap AS price, total_volume AS volume
            FROM vwap_history
            WHERE window_len = '1 min'
            """,
            conn,
        )
    finally:
        conn.close()
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=FEATURES).copy()
    df = df[(df["price"] > 0) & (df["volume"] > 0)]
    return df


def main():
    df = load_training_data(DB_PATH)
    print(f"Wierszy surowych: {len(df)}")

    df = clean(df)
    print(f"Wierszy po czyszczeniu: {len(df)}")

    X = df[FEATURES].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=200,
        contamination=CONTAMINATION,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    preds = model.predict(X_scaled)
    n_anom = int((preds == -1).sum())
    print(f"Anomalie: {n_anom}/{len(df)} ({100 * n_anom / len(df):.1f}%)")
    print(f"Statystyki cech:\n{df[FEATURES].describe()}")

    bundle = {
        "model": model,
        "scaler": scaler,
        "features": FEATURES,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "n_samples": len(df),
        "contamination": CONTAMINATION,
    }
    joblib.dump(bundle, MODEL_PATH)


if __name__ == "__main__":
    main()
