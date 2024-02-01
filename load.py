from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
import pandas as pd
from extract import *

postgres_conn_string = "postgresql://postgres:postgres@localhost:5432/vaccine_stock_analysis"

@op
def load_distribution(empty):
    engine = create_engine(postgres_conn_string)
    DistributionDataFrame = pd.read_csv("staging/transformed_distribution.csv")
    DistributionDataFrame.to_sql("distribution_table",
    schema = "public",
    con = engine,
    index = False,
    if_exists = "replace"
    )
    engine.dispose(close=True)

def load_prices(empty):
    engine = create_engine(postgres_conn_string)
    StockPriceDataFrame = pd.read_csv("staging/StockPriceDataFrame.csv")
    StockPriceDataFrame.to_sql("price_table",
    schema = "public",
    con = engine,
    index = False,
    if_exists = "replace"
    )
    engine.dispose(close=True)