"""
Pipeline execution engine.
"""
from pyspark.sql import SparkSession, DataFrame, functions as F

from src.core.config import PipelineConfig
from src.transforms.base import *

class PipelineEngine:
    """Executes a transformation pipeline."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def _read_source(self) -> DataFrame:
        return (self.spark.read
                .option("header", True)
                .format(self.config.source["type"])
                .load(self.config.source["path"])
                )

    def run(self) -> DataFrame:
        """Execute the pipeline and return the result."""
        # raise NotImplementedError
        df = self._read_source()
        print(f"")
        df.show()

        for step_config in self.config.transformation_steps:
            print(f"step_config : {step_config}")

            if step_config['type'] == "clean":
                print("before changes", df.count())
                clean_columns = CleanColumns(step_config)
                df = clean_columns.apply(df)
                print("after changes", df.count())
            elif step_config['type'] == "filter":
                print("before changes", df.count())
                required_filter = FilterTransform(step_config)
                df = required_filter.apply(df)
                print("after changes", df.count())
            elif step_config['type'] == "expression":
                print("before changes", df.count())
                required_expression = ExpressionTransform(step_config)
                df = required_expression.apply(df)
                print("after changes", df.count())
            elif step_config['type'] == "aggregate":
                print("before changes", df.count())
                required_aggregate = AggregateTransform(step_config)
                df = required_aggregate.apply(df)
                print("after changes", df.count())
            else:
                pass

        return df

if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("engine").master("local").getOrCreate()
    config_file = "/Users/bakulseth/PycharmProjects/coding-test-data-engineer-main/config/pipeline.yaml"

    pipeline_config = PipelineConfig.from_yaml(config_file)
    engine = PipelineEngine(spark_session, pipeline_config)

    result_df = engine.run()
    result_df.show()
    result_df.printSchema()
    print(f"Total rows in the dataframe : {result_df.count()}")