"""
Transform base.
"""
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame, functions as F


class Transform(ABC):
    """Base class for transformations."""
    def __init__(self, config: dict):
        self.config = config
        self.name = config["name"]

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        """Apply the transformation."""
        pass

class CleanColumns(Transform):

    def apply(self, df: DataFrame) -> DataFrame:
        operations = self.config["operations"]
        for operation in operations:
            match operation["function"]:
                case "trim_all":
                    df.printSchema()
                    print(f"Trimming all the string columns")
                    df.printSchema()
                    df = df.select([
                        F.trim(F.col(col_name)).alias(col_name)
                        if col_type == "string" else F.col(col_name)
                        for col_name, col_type in df.dtypes
                    ])
                case "upper":
                    columns = operation["columns"]
                    print(f"converting {columns} to upper case")
                    df = df.select([
                        F.upper(F.col(col_name)).alias(col_name)
                        if col_name in columns else F.col(col_name)
                        for col_name in df.columns
                    ])
                case "cast":
                    for inp in operation["input"]:
                        col_name = inp['column']
                        to_cast = inp['to']
                        if to_cast == "double" or to_cast == "long":
                            print(f"converting column {col_name} to {to_cast}")
                            df = df.withColumn(col_name, F.expr(f"try_cast(`{col_name}` AS {to_cast})"))
                        elif to_cast == "date":
                            format1, format2 = inp['format']
                            print(f"converting column {col_name} to {to_cast} and {format1} and {format2}")

                            df = df.withColumn(
                                col_name, F.coalesce(F.try_to_date(col_name, format1), F.try_to_date(col_name, format2))
                            )
                    print(f" Count after all the operations : {df.count()}")
                    print("schema after casting")
                    df.printSchema()


        return df

class FilterTransform(Transform):

    def apply(self, df: DataFrame) -> DataFrame:
        column = self.config["condition"]["column"]
        operator = self.config["condition"]["operator"]
        value = self.config["condition"]["value"]
        match operator:
            case "gt":
                print(f"Applying {operator} on column {column} against value {value}")
                return df.filter(F.col(column) > F.lit("0"))

        return df

class ExpressionTransform(Transform):

    def apply(self, df: DataFrame) -> DataFrame:
        spec = self.config['expression']
        out_col = self.config['output_column']
        function = spec['function']
        columns = [F.col(i["column"]) for i in spec.get("inputs", [])]

        if function == "multiply":
            print(f"applying {spec} on {columns}")
            return df.withColumn(out_col, F.try_multiply(columns[0] , columns[1]))
        elif function == "divide":
            print(f"applying {spec} on {columns}")
            return df.withColumn(out_col, F.try_divide(columns[0] , columns[1]))
        return df


class AggregateTransform(Transform):
    FUNCTIONS = {"sum": F.sum, "avg": F.avg, "count": F.count, "min": F.min, "max": F.max}

    def apply(self, df: DataFrame) -> DataFrame:
        agg = [self.FUNCTIONS[a['function']](F.col(a['column'])).alias(a['alias'])
               for a in self.config['aggregations']]
        df = df.groupby(*self.config['group_by']).agg(*agg)
        print(f"Applying aggregations on the dataframes ")
        df.show()
        df.printSchema()
        return df


if __name__ == "__main":
    spark = SparkSession.builder.appName("tfx").master("local").getOrCreate()
    data = [
        ("Alice", 25),
        ("Bob", 15),
        ("Charlie", 30),
        ("David", 12)
    ]
    columns = ["name", "age"]
    df_input = spark.createDataFrame(data, schema=columns)
    df_input.show()
    config_data = {
        "name": "AgeFilterTask",
        "condition": {
            "column": "age",
            "operator": "gt",
            "value": 18
        }
    }

    # D. Instantiate and run your transformation class!
    age_filter = FilterTransform(config=config_data)
    df_output = age_filter.apply(df_input)
    print("--- Filtered DataFrame (Age > 18) ---")
    df_output.show()

