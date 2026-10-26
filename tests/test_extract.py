"""Unit and integration tests for market data extraction and S3 ingestion.

These tests use pytest, unittest.mock, and moto to simulate AWS S3 locally,
ensuring 100% cloud-free testing with zero AWS charges or dependencies.
"""

import io
import os
from unittest.mock import MagicMock, patch

import boto3
import pandas as pd
import pytest

try:
    from moto import mock_aws
except ImportError:
    from moto import mock_s3 as mock_aws

from src.extract_s3 import (
    MarketDataExtractor,
    PipelineConfig,
    S3DataLakeService,
    TechMarketExtractionPipeline,
)

# Test Constants
TEST_BUCKET_NAME = "test-market-data-lake-raw"
TEST_REGION = "us-east-1"


@pytest.fixture(autouse=True)
def aws_credentials_env(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing_access_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing_secret_key")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing_security_token")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing_session_token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", TEST_REGION)
    monkeypatch.setenv("S3_BUCKET_NAME", TEST_BUCKET_NAME)


@pytest.fixture
def sample_market_df() -> pd.DataFrame:
    """Fixture providing a realistic mock financial DataFrame from yfinance."""
    dates = pd.date_range(start="2026-09-01", periods=10, freq="D")
    data = {
        "Open": [120.5 + i for i in range(10)],
        "High": [125.0 + i for i in range(10)],
        "Low": [119.0 + i for i in range(10)],
        "Close": [124.0 + i for i in range(10)],
        "Volume": [50000000 + (i * 1000000) for i in range(10)],
        "Dividends": [0.0] * 10,
        "Stock Splits": [0.0] * 10,
    }
    df = pd.DataFrame(data, index=dates)
    df.index.name = "Date"
    return df


@pytest.fixture
def mock_s3_bucket():
    """Moto fixture to create a clean, mocked S3 bucket."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=TEST_REGION)
        s3.create_bucket(Bucket=TEST_BUCKET_NAME)
        yield s3


class TestMarketDataExtractor:
    """Tests for the MarketDataExtractor logic and data quality validation."""

    def test_validation_passes_with_valid_data(self, sample_market_df):
        """Verify that validation passes when all required financial columns are present."""
        extractor = MarketDataExtractor()
        # Should not raise any exception
        extractor.validate_extracted_data(sample_market_df, "NVDA")

    def test_validation_raises_on_empty_dataframe(self):
        """Verify that an empty DataFrame raises a ValueError."""
        extractor = MarketDataExtractor()
        empty_df = pd.DataFrame()
        with pytest.raises(ValueError, match="empty"):
            extractor.validate_extracted_data(empty_df, "NVDA")

    def test_validation_raises_on_missing_columns(self):
        """Verify that a DataFrame missing essential columns raises a ValueError."""
        extractor = MarketDataExtractor()
        incomplete_df = pd.DataFrame({
            "Open": [100.0, 102.0],
            "Volume": [1000, 2000],
            # Missing High, Low, Close
        })
        with pytest.raises(ValueError, match="missing expected columns"):
            extractor.validate_extracted_data(incomplete_df, "NVDA")

    @patch("yfinance.Ticker")
    def test_fetch_ticker_data_success(self, mock_ticker_class, sample_market_df):
        """Test successful download and standardization of market data."""
        mock_instance = MagicMock()
        mock_instance.history.return_value = sample_market_df
        mock_ticker_class.return_value = mock_instance

        extractor = MarketDataExtractor(period="1mo", interval="1d")
        result_df = extractor.fetch_ticker_data("NVDA")

        # Verify yfinance called with expected parameters
        mock_instance.history.assert_called_once_with(period="1mo", interval="1d")

        # Verify standardized schema
        assert "Date" in result_df.columns
        assert "ticker" in result_df.columns
        assert "ingested_at_utc" in result_df.columns
        assert (result_df["ticker"] == "NVDA").all()
        assert len(result_df) == 10


class TestS3DataLakeService:
    """Tests for S3DataLakeService using moto for AWS emulation."""

    def test_upload_parquet_to_s3(self, mock_s3_bucket, sample_market_df):
        """Test serializing DataFrame to Parquet and uploading to S3."""
        config = PipelineConfig(bucket_name=TEST_BUCKET_NAME, storage_format="parquet")
        s3_service = S3DataLakeService(config=config, s3_client=mock_s3_bucket)

        sample_market_df_clean = sample_market_df.reset_index()
        sample_market_df_clean["ticker"] = "NVDA"

        s3_uri = s3_service.upload_dataframe(sample_market_df_clean, ticker="NVDA", storage_format="parquet")

        assert s3_uri.startswith(f"s3://{TEST_BUCKET_NAME}/bronze/market_data/ticker=NVDA/")
        assert s3_uri.endswith(".parquet")

        # Verify the file was physically saved in the mock S3 bucket
        key = s3_uri.replace(f"s3://{TEST_BUCKET_NAME}/", "")
        response = mock_s3_bucket.get_object(Bucket=TEST_BUCKET_NAME, Key=key)

        downloaded_bytes = response["Body"].read()
        downloaded_df = pd.read_parquet(io.BytesIO(downloaded_bytes))

        assert len(downloaded_df) == 10
        assert "Close" in downloaded_df.columns
        assert (
            response["Metadata"].get("pipeline_stage") == "bronze_raw"
            or response["Metadata"].get("pipeline-stage") == "bronze_raw"
        )

    def test_upload_csv_to_s3(self, mock_s3_bucket, sample_market_df):
        """Test serializing DataFrame to CSV and uploading to S3."""
        config = PipelineConfig(bucket_name=TEST_BUCKET_NAME, storage_format="csv")
        s3_service = S3DataLakeService(config=config, s3_client=mock_s3_bucket)

        sample_market_df_clean = sample_market_df.reset_index()
        sample_market_df_clean["ticker"] = "MSFT"

        s3_uri = s3_service.upload_dataframe(sample_market_df_clean, ticker="MSFT", storage_format="csv")

        assert s3_uri.endswith(".csv")

        key = s3_uri.replace(f"s3://{TEST_BUCKET_NAME}/", "")
        response = mock_s3_bucket.get_object(Bucket=TEST_BUCKET_NAME, Key=key)
        downloaded_bytes = response["Body"].read()
        downloaded_df = pd.read_csv(io.BytesIO(downloaded_bytes))

        assert len(downloaded_df) == 10
        assert "Volume" in downloaded_df.columns

    def test_check_bucket_exists(self, mock_s3_bucket):
        """Test S3 bucket existence check."""
        config = PipelineConfig(bucket_name=TEST_BUCKET_NAME)
        s3_service = S3DataLakeService(config=config, s3_client=mock_s3_bucket)

        assert s3_service.check_bucket_exists(TEST_BUCKET_NAME) is True
        assert s3_service.check_bucket_exists("non-existent-bucket-xyz") is False


class TestTechMarketExtractionPipeline:
    """End-to-End Orchestrator Pipeline Tests."""

    @patch("yfinance.Ticker")
    def test_pipeline_run_all_success(self, mock_ticker_class, mock_s3_bucket, sample_market_df):
        """Test end-to-end extraction and S3 ingestion for NVDA, TSM, MSFT."""
        mock_instance = MagicMock()
        mock_instance.history.return_value = sample_market_df
        mock_ticker_class.return_value = mock_instance

        config = PipelineConfig(
            bucket_name=TEST_BUCKET_NAME,
            target_tickers=["NVDA", "TSM", "MSFT"],
            storage_format="parquet",
        )
        s3_service = S3DataLakeService(config=config, s3_client=mock_s3_bucket)
        pipeline = TechMarketExtractionPipeline(config=config, s3_service=s3_service)

        report = pipeline.run()

        assert report["status"] == "SUCCESS"
        assert len(report["failed_tickers"]) == 0
        assert len(report["tickers_processed"]) == 3
        for ticker in ["NVDA", "TSM", "MSFT"]:
            assert ticker in report["tickers_processed"]
            assert report["tickers_processed"][ticker]["records"] == 10
            assert report["tickers_processed"][ticker]["s3_uri"].startswith(f"s3://{TEST_BUCKET_NAME}/")

    @patch("yfinance.Ticker")
    def test_pipeline_partial_failure_resilience(self, mock_ticker_class, mock_s3_bucket, sample_market_df):
        """Verify pipeline continues processing even if one ticker fails."""
        def side_effect(ticker_symbol):
            mock = MagicMock()
            if ticker_symbol == "INVALID_TICKER":
                mock.history.return_value = pd.DataFrame()  # Triggers empty dataframe validation error
            else:
                mock.history.return_value = sample_market_df
            return mock

        mock_ticker_class.side_effect = side_effect

        config = PipelineConfig(
            bucket_name=TEST_BUCKET_NAME,
            target_tickers=["NVDA", "INVALID_TICKER", "MSFT"],
        )
        s3_service = S3DataLakeService(config=config, s3_client=mock_s3_bucket)
        pipeline = TechMarketExtractionPipeline(config=config, s3_service=s3_service)

        report = pipeline.run()

        assert report["status"] == "PARTIAL_FAILURE"
        assert "NVDA" in report["tickers_processed"]
        assert "MSFT" in report["tickers_processed"]
        assert len(report["failed_tickers"]) == 1
        assert report["failed_tickers"][0]["ticker"] == "INVALID_TICKER"
