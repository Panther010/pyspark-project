"""
Pipeline execution engine.
"""
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F

from src.core.config import PipelineConfig
from src.transforms.base import (
    Transform,
    CleanColumns,
    FilterTransform,
    ExpressionTransform,
    AggregateTransform
)
from src.transforms.functions import CHECKS

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
        self.log.info("Reading data from '%s'", self.config.source["path"])
        return (self.spark.read
                .option("header", True)
                .format(self.config.source["type"])
                .load(self.config.source["path"])
                )

    def _run_transformation(self, df: DataFrame, step: Transform) -> DataFrame:
        bad = step.rejected(df)
        if bad is not None:
            self.rejected_df = bad if self.rejected_df is None else self.rejected_df.unionByName(bad)

        return step.apply(df)

    def _validate(self, df:DataFrame) -> None:
        rules = self.config.validation_rules
        if not rules:
            return

        exprs = []
        for rule in rules['rules']:
            check = CHECKS.get(rule["check"])
            name = f"{rule['column']}:{rule['check']}"
            exprs.append(F.sum(F.when(~check(rule["column"]), 1).otherwise(0)).alias(name))

        counts = df.agg(*exprs)
        counts.show()
        counts1 = counts.first().asDict()

        failed = {rule: n for rule, n in counts1.items() if n}

        if failed:
            self.log.error(f"Output validation failed (rule: violating rows) {failed}")
        self.log.info("Output validation passed (%d rules)", len(rules['rules']))


    def run(self) -> DataFrame:
        """Execute the pipeline and return the result."""
        steps = [self._build_step(c) for c in self.config.transformation_steps]
        # Reading data
        df = self._read_source()

        # Applying transformations
        self.log.info("Running transformation steps ")
        for step in steps:
            self.log.info("Running step '%s'", step.name)
            df = self._run_transformation(df, step)

        # Selecting required columns
        target_columns = self.config.output_columns.get('columns', [])
        self.log.info("Selecting columns '%s' from result ", target_columns)
        if target_columns:
            df = df.select(*self.config.output_columns['columns'])

        # Running validations
        self._validate(df)

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