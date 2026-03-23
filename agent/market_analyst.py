"""
Market Analyst Agent — LangChain agent that reads dbt analytics models
and generates natural language market briefs grounded in real data.

Usage:
    python agent/market_analyst.py
    # or via API:
    python agent/api.py
"""
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain.tools import tool
from langchain_core.prompts import ChatPromptTemplate
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a quantitative market analyst with access to
real-time sentiment and price data from a streaming pipeline.

When asked about a stock or market trend, you:
1. Query the relevant data tables using your SQL tool
2. Analyze the numbers carefully
3. Generate a concise, data-backed market brief

Always cite specific numbers from the data. Never speculate beyond
what the data shows. If data is insufficient, say so clearly.

Available tables in the analytics schema:
- analytics.sentiment_by_symbol — 5-min bucketed sentiment scores per ticker
- analytics.trending_tickers    — tickers ranked by mention velocity
- analytics.price_sentiment_correlation — hourly price change vs sentiment

Format your brief as:
- **Symbol:** [TICKER]
- **Sentiment:** [current trend and score]
- **Price:** [current price and recent change]
- **Signal:** [what the data suggests]
- **Confidence:** [low/medium/high based on data volume]
"""


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("TIMESCALE_HOST", "localhost"),
        port=int(os.environ.get("TIMESCALE_PORT", 5432)),
        dbname=os.environ.get("TIMESCALE_DB", "sentiment_db"),
        user=os.environ.get("TIMESCALE_USER", "sentiment_user"),
        password=os.environ.get("TIMESCALE_PASSWORD"),
    )


@tool
def query_sentiment_data(sql: str) -> str:
    """
    Execute a SQL query against the analytics schema (dbt mart models).
    Available tables: analytics.sentiment_by_symbol, analytics.trending_tickers,
    analytics.price_sentiment_correlation.
    Always use the analytics schema prefix.
    Returns results as a formatted string.
    """
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(sql, conn)
        conn.close()
        if df.empty:
            return "No data found for this query."
        return df.to_string(index=False, max_rows=20)
    except Exception as e:
        return f"Query error: {e}"


def build_agent() -> AgentExecutor:
    llm = ChatAnthropic(
        model="claude-haiku-4-5-20251001",
        api_key=os.environ.get("ANTHROPIC_API_KEY"),
        temperature=0,
    )
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ])
    agent = create_tool_calling_agent(llm, [query_sentiment_data], prompt)
    return AgentExecutor(agent=agent, tools=[query_sentiment_data], verbose=True)


def get_market_brief(question: str) -> str:
    executor = build_agent()
    result = executor.invoke({"input": question})
    return result["output"]


if __name__ == "__main__":
    brief = get_market_brief(
        "What is the current sentiment for NVDA and has price moved in the same direction?"
    )
    print(brief)
