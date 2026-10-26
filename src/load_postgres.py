"""Market Data Warehouse Ingestion (Gold Layer).

This module will implement:
- SQLAlchemy ORM models for structured financial time series.
- Upsert (INSERT ... ON CONFLICT DO UPDATE) logic to prevent duplicates.
- Efficient batch insertion into PostgreSQL for consumption by Streamlit or FastAPI.

Targeted for implementation in Phase 3.
"""

from typing import Any, Dict
