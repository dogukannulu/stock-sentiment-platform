"""
FastAPI wrapper for the Market Analyst Agent.

Run with:
    pip install fastapi uvicorn
    uvicorn agent.api:app --reload --port 8000

Endpoints:
    POST /brief  — generate a market brief
    GET  /health — health check
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from agent.market_analyst import get_market_brief
import logging

logger = logging.getLogger(__name__)
app = FastAPI(title="Market Analyst Agent API", version="1.0.0")


class BriefRequest(BaseModel):
    question: str


class BriefResponse(BaseModel):
    question: str
    brief: str


@app.post("/brief", response_model=BriefResponse)
async def get_brief(request: BriefRequest) -> BriefResponse:
    """Generate a market brief for the given question."""
    try:
        brief = get_market_brief(request.question)
        return BriefResponse(question=request.question, brief=brief)
    except Exception as e:
        logger.error(f"Agent error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
