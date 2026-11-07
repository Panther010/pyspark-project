"""
Transform base.
"""
from abc import ABC, abstractmethod
import logging
from src.transforms.functions import (
    EXPRESSION_FUNCTIONS,
    FILTER_OPERATORS,
    trim_all,
    to_upper,
    safe_cast,
    parse_date
)
from pyspark.sql import DataFrame, functions as F


class Transform(ABC):
    """Base class for transformations."""
    def __init__(self, config: dict):
        self.config = config
        self.name = config["name"]
        self.log = logging.getLogger(__name__)

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        """Apply the transformation."""
        pass

    @abstractmethod
    def rejected(self, df:DataFrame):
        return None


class CleanColumns(Transform):

    def apply(self, df: DataFrame) -> DataFrame:
        operations = self.config["operations"]
        for operation in operations:
            match operation["function"]:
                case "trim_all":
                    self.log.info("Trimming all the string columns")
                    df = trim_all(df)
                case "upper":
                    columns = operation["columns"]
                    self.log.info("converting '%s' to upper case", columns)
                    df = to_upper(df, columns)
                case "cast":
                    for inp in operation["input"]:
                        col_name = inp['column']
                        to_cast = inp['to']

                        if to_cast == "date":
                            self.log.info("converting column '%s' to '%s'", col_name, to_cast)
                            df = df.withColumn(col_name, parse_date(col_name, inp["format"]))
                        else:
                            self.log.info("converting column '%s' to '%s'", col_name, to_cast)
                            df = df.withColumn(col_name, safe_cast(col_name, to_cast))
                case _:
                    raise ValueError(
                f"{self.name}: unknown operation '{operation}'. Supported: ['cast', 'trim', 'upper']")
        df.show(30)
        return df

    def rejected(self, df:DataFrame):
        return None

class FilterTransform(Transform):

    def _pass(self):
        column = self.config["condition"]["column"]
        operator = self.config["condition"]["operator"]
        value = self.config["condition"].get('value')

        if operator not in FILTER_OPERATORS:
            raise ValueError(f"{self.name}: unknown operator '{operator}'. Supported: {sorted(FILTER_OPERATORS)}")

        self.log.info("Applying '%s' on column : '%s'", operator, column)

        return FILTER_OPERATORS[operator](column, value)

    def apply(self, df: DataFrame) -> DataFrame:

        passed = df.filter(self._pass())
        return passed

    def rejected(self, df:DataFrame):
        return df.filter(~self._pass()).withColumn("reject_reason", F.lit(self.config.get('reject_reason')))

class ExpressionTransform(Transform):

    def apply(self, df: DataFrame) -> DataFrame:
        spec = self.config['expression']
        out_col = self.config['output_column']
        function = spec['function']
        columns = [F.col(i["column"]) for i in spec.get("inputs", [])]

        fn = EXPRESSION_FUNCTIONS.get(function)

        if fn is None:
            raise ValueError(
                f"{self.name}: unknown function '{function}'. Supported: {sorted(EXPRESSION_FUNCTIONS)}")
        return df.withColumn(out_col, fn(columns))

    def rejected(self, df:DataFrame):
        return None

class AggregateTransform(Transform):
    FUNCTIONS = {"sum": F.sum, "avg": F.avg, "count": F.count, "min": F.min, "max": F.max}

    def apply(self, df: DataFrame) -> DataFrame:
        agg = [self.FUNCTIONS[a['function']](F.col(a['column'])).alias(a['alias'])
               for a in self.config['aggregations']]
        df = df.groupby(*self.config['group_by']).agg(*agg)
        self.log.info("Applying aggregations on Dataframe ")
        return df

    def rejected(self, df:DataFrame):
        return None
