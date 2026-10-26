"""Unit tests for data transformation and validation logic (Silver Layer).

Validates Pydantic schema constraints and preliminary transformation logic
according to Phase 2 of the Medallion Architecture.
"""

import pytest
from pydantic import ValidationError

from src.transform import MarketRecordSchema


class TestMarketRecordSchema:
    """Test suite for MarketRecordSchema Pydantic validation."""

    def test_valid_market_record_passes(self):
        """Verify that a valid financial record passes schema validation."""
        valid_data = {
            "Date": "2026-10-25",
            "ticker": "NVDA",
            "Open": 135.50,
            "High": 138.20,
            "Low": 134.10,
            "Close": 137.80,
            "Volume": 45000000,
        }
        record = MarketRecordSchema(**valid_data)
        assert record.ticker == "NVDA"
        assert record.Close == 137.80
        assert record.Volume == 45000000

    def test_negative_price_raises_validation_error(self):
        """Verify that negative stock prices trigger validation errors."""
        invalid_data = {
            "Date": "2026-10-25",
            "ticker": "NVDA",
            "Open": -10.0,  # Invalid: must be > 0
            "High": 138.20,
            "Low": 134.10,
            "Close": 137.80,
            "Volume": 45000000,
        }
        with pytest.raises(ValidationError):
            MarketRecordSchema(**invalid_data)

    def test_negative_volume_raises_validation_error(self):
        """Verify that negative trading volume triggers a validation error."""
        invalid_data = {
            "Date": "2026-10-25",
            "ticker": "TSM",
            "Open": 170.0,
            "High": 175.0,
            "Low": 168.0,
            "Close": 172.0,
            "Volume": -100,  # Invalid: must be >= 0
        }
        with pytest.raises(ValidationError):
            MarketRecordSchema(**invalid_data)

    def test_missing_required_field_raises_validation_error(self):
        """Verify that omitting a required field triggers a validation error."""
        incomplete_data = {
            "Date": "2026-10-25",
            "ticker": "MSFT",
            "Open": 425.0,
            # Missing High, Low, Close, Volume
        }
        with pytest.raises(ValidationError):
            MarketRecordSchema(**incomplete_data)
