import os
from contextlib import asynccontextmanager

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

MODEL_PATH = os.getenv("MODEL_PATH", "model.pkl")
STATE: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    bundle = joblib.load(MODEL_PATH)
    STATE["bundle"] = bundle
    print(f"samlpes: {bundle['n_samples']}, features: {bundle['features']}")
    yield
    STATE.clear()


app = FastAPI(title="anomaly detection", version="1.0", lifespan=lifespan)


class PredictRequest(BaseModel):
    price: float = Field(..., gt=0, description="Price")
    volume: float = Field(..., ge=0, description="Volume")


class PredictResponse(BaseModel):
    anomaly: bool
    score: float


@app.get("/health")
def health():
    bundle = STATE.get("bundle")
    if bundle is None:
        return {"status": "no_model", "model": None}
    return {
        "status": "ok",
        "model": {
            "features": bundle["features"],
            "n_samples": bundle["n_samples"],
            "trained_at": bundle["trained_at"],
            "contamination": bundle["contamination"],
        },
    }


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    bundle = STATE.get("bundle")
   
    model = bundle["model"]
    scaler = bundle["scaler"]
    features = bundle["features"]

    row = {"price": req.price, "volume": req.volume}
    X = np.array([[row[f] for f in features]], dtype=float)

    X_scaled = scaler.transform(X)

    pred = int(model.predict(X_scaled)[0])
    score = float(-model.decision_function(X_scaled)[0])

    return PredictResponse(anomaly=(pred == -1), score=round(score, 4))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
