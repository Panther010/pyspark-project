"""
Pipeline execution engine.
"""
import logging
from pyspark.sql import SparkSession, DataFrame

from src.core.config import PipelineConfig
from src.transforms.base import (
    CleanColumns,
    FilterTransform,
    ExpressionTransform,
    AggregateTransform
)


TRANSFORMS = {
    "clean": CleanColumns,
    "filter": FilterTransform,
    "expression": ExpressionTransform,
    "aggregate": AggregateTransform,
}

class PipelineEngine:
    """Executes a transformation pipeline."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.log = logging.getLogger(__name__)
        self.rejected_df = None

    @staticmethod
    def _build_step(step_config:dict) -> Transform:
        step_type = step_config.get("type")
        step_class = TRANSFORMS.get(step_type)

        if step_class is None:
            raise ValueError(
                f"Step '{step_config.get('name')}' has unknown type '{step_type}'. "
                f"Supported types: {sorted(TRANSFORMS)}"
            )

        return step_class(step_config)


    def _read_source(self) -> DataFrame:
        return (self.spark.read
                .option("header", True)
                .format(self.config.source["type"])
                .load(self.config.source["path"])
                )

    def run(self) -> DataFrame:
        """Execute the pipeline and return the result."""
        # raise NotImplementedError
        steps = [self._build_step(c) for c in self.config.transformation_steps]

        df = self._read_source()

        for step in steps:
            self.log.info("Running step '%s'", step.name)

            bad = step.rejected(df)
            if bad is not None:
                self.rejected_df = bad if self.rejected_df is None else self.rejected_df.unionByName(bad)

            df = step.apply(df)

        df = df.select(*self.config.output_columns['columns'])

        return df

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    )
    spark_session = SparkSession.builder.appName("engine").master("local").getOrCreate()
    config_file = "/Users/bakulseth/PycharmProjects/coding-test-data-engineer-main/config/pipeline.yaml"

    pipeline_config = PipelineConfig.from_yaml(config_file)
    engine = PipelineEngine(spark_session, pipeline_config)

    result_df = engine.run()
    result_df.show()
    result_df.printSchema()
    print(f"Total rows in the dataframe : {result_df.count()}")