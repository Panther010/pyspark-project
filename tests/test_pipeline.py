"""
Pipeline tests.
"""
import pytest
from pyspark.sql import SparkSession

from src.core import PipelineConfig, PipelineEngine


class TestPipeline:
    """Tests for the transformation pipeline."""

    def test_pipeline_produces_output(self, spark: SparkSession):
        """The pipeline should produce a non-empty result."""
        config = PipelineConfig.from_yaml("config/pipeline.yaml")
        engine = PipelineEngine(spark, config)

        result = engine.run()

        assert result is not None
        assert result.count() > 0

    def test_output_has_expected_columns(self, spark: SparkSession):
        """Output should contain the columns specified in config."""
        config = PipelineConfig.from_yaml("config/pipeline.yaml")
        engine = PipelineEngine(spark, config)

        result = engine.run()

        assert "instrument_id" in result.columns
        assert "vwap" in result.columns

    def test_no_duplicate_instrument_date_pairs(self, spark: SparkSession):
        """Each instrument/date combination should appear at most once."""
        config = PipelineConfig.from_yaml("config/pipeline.yaml")
        engine = PipelineEngine(spark, config)

        result = engine.run()

        total = result.count()
        distinct = result.select("instrument_id", "trade_date").distinct().count()
        assert total == distinct
