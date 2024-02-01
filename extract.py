from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
import math

mongo_conn_str = 'mongodb+srv://dap:dap@cluster0.22s8qf8.mongodb.net'

DistributionDataFrame = create_dagster_pandas_dataframe_type(
    name = "DistributionDataFrame",
    columns = [
        PandasColumn.string_column("date", non_nullable = True),
        PandasColumn.integer_column("distributed_janssen", non_nullable = True),
        PandasColumn.integer_column("distributed_moderna", non_nullable = True),
        PandasColumn.integer_column("distributed_pfizer", non_nullable = True),
        PandasColumn.float_column("distributed_novavax", non_nullable = False),
    ]
)

StockPriceDataFrame = create_dagster_pandas_dataframe_type(
    name = "StockPriceDataFrame",
    columns = [
        PandasColumn.string_column("Date", non_nullable = True),
        PandasColumn.string_column("company", non_nullable = True),
        PandasColumn.float_column("Open", non_nullable = False),
        PandasColumn.float_column("High", non_nullable = False),
        PandasColumn.float_column("Low", non_nullable = False),
        PandasColumn.float_column("Close", non_nullable = False),
        PandasColumn.float_column("Volume", non_nullable = False)
        
    ]
)

@op(ins={'start': In(bool)}, out=Out(DistributionDataFrame))
def extract_distribution(start) -> DistributionDataFrame:
    conn = MongoClient(mongo_conn_str)
    db = conn["dap_project"]
    distribution = pd.DataFrame(db['vaccine_distribution'].find({}))
    # change the data_type
    distribution = distribution[["date", "distributed_janssen", "distributed_moderna",
    "distributed_pfizer", "distributed_novavax"]]
    distribution["date"] = list(map(lambda x: str(datetime.strptime(x[0:-13], '%Y-%m-%d').date()), distribution["date"]))
    distribution["distributed_janssen"] = list(map(lambda x: int(x), distribution["distributed_janssen"]))
    distribution["distributed_moderna"] = list(map(lambda x: int(x), distribution["distributed_moderna"]))
    distribution["distributed_pfizer"] = list(map(lambda x: int(x), distribution["distributed_pfizer"]))
    distribution["distributed_novavax"] = list(map(lambda x:  float(0) if math.isnan(float(x)) == True else float(x), distribution["distributed_novavax"]))
    distribution.to_csv("staging/distribution.csv") #save the distributiondataFrame to staging
    conn.close()
    return distribution

@op(ins={'start': In(bool)}, out=Out(StockPriceDataFrame))
def extract_price(start) -> StockPriceDataFrame:
    conn = MongoClient(mongo_conn_str)
    db = conn["dap_project"]
    price = pd.DataFrame(db['vaccine_stock_price'].find({}))
    price = price[['Date', 'High', 'Open', 'Close', 'Low', 'Volume', 'company']]
    price['date'] = list(map(lambda x: str(datetime.strptime(x[0:-14], '%Y-%m-%d').date()), price["date"]))
    price['High'] = list(map(lambda x: float(x), price['High']))
    price['Open'] = list(map(lambda x: float(x), price['Open']))
    price['Close'] = list(map(lambda x: float(x), price['Close']))
    price['Low'] = list(map(lambda x: float(x), price['Low']))
    price['Volume'] = list(map(lambda x: float(x), price['Volume']))
    price.to_csv("staging/price.csv") #save the StockPriceDataFrame to staging

@op(ins={'start': In(bool)}, out=Out(StockPriceDataFrame))
def extract_price(start) -> StockPriceDataFrame:
    conn = MongoClient(mongo_conn_str)
    db = conn["dap_project"]
    price = pd.DataFrame(db['vaccine_stock_price'].find({}))
    price = price[['Date', 'High', 'Open', 'Close', 'Low', 'Volume', 'company']]
    price['date'] = list(map(lambda x: str(datetime.strptime(x[0:-14], '%Y-%m-%d').date()), price["date"]))
    price['High'] = list(map(lambda x: float(x), price['High']))
    price['Open'] = list(map(lambda x: float(x), price['Open']))
    price['Close'] = list(map(lambda x: float(x), price['Close']))
    price['Low'] = list(map(lambda x: float(x), price['Low']))
    price['Volume'] = list(map(lambda x: float(x), price['Volume']))
    price.to_csv("staging/price.csv") #save the StockPriceDataFrame to staging


print(DistributionDataFrame)