"""
Entry point for running the transformation pipeline.
"""
import logging
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    )

    spark = create_spark_session()

    config = PipelineConfig.from_yaml("config/pipeline.yaml")
    engine = PipelineEngine(spark, config)

    result_df = engine.run()

    if engine.rejected_df is not None:
        logging.info("\n")
        logging.info("Rejected data and details areas follows")
        engine.rejected_df.show(truncate=False)

    logging.info("Final calculated result is")
    result_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
