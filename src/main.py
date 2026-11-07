"""
Entry point for running the transformation pipeline.
"""
from pyspark.sql import SparkSession

from src.core import PipelineEngine, PipelineConfig


def create_spark_session() -> SparkSession:
    """Create a local Spark session for development."""
    return (
        SparkSession.builder
        .appName("instrument_pipeline")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    config = PipelineConfig.from_yaml("config/pipeline.yaml")
    engine = PipelineEngine(spark, config)

    result_df = engine.run()

    result_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
