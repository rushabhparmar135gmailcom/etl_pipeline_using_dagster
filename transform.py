from dagster import op, Out, In, DagsterType
import pandas as pd
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type


TransformedDistributionDataFrame = create_dagster_pandas_dataframe_type(
    name = "TransformedDistributionDataFrame",
    columns = [
        PandasColumn.string_column("date", non_nullable = True),
        PandasColumn.integer_column("distributed_janssen", non_nullable = True),
        PandasColumn.integer_column("distributed_moderna", non_nullable = True),
        PandasColumn.integer_column("distributed_pfizer", non_nullable = True),
        PandasColumn.float_column("distributed_novavax", non_nullable = False),

    ]
)

@op (ins = {'start':In(None)}, out = Out(TransformedDistributionDataFrame))
def transform_distribution(start) -> TransformedDistributionDataFrame:
    
    distribution = pd.read_csv("staging/distribution.csv")
    distribution_grouped = distribution.groupby("date").sum()
    distribution_grouped['date'] = distribution_grouped.index
    return distribution_grouped

@op (ins = {'distribution': In(TransformedDistributionDataFrame)}, out = Out(None))
def stage_transformed_distribution(distribution):
    distribution.to_csv("staging/transformed_distribution.csv")
