import webbrowser
from dagster import job
from extract import *
from transform import *
from load import *


@job
def etl():
    load_distribution(
    stage_transformed_distribution(
    transform_distribution(
    extract_distribution()
    )
    )
    )