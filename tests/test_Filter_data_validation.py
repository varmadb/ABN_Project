from Application import rename_Column, filtering_df
import pytest
from pyspark.sql.types import *
from chispa.schema_comparer import *
from chispa.dataframe_comparer import *
from tests.spark import spark


def test_df_filetered_data_compare ():
    input_df = spark.createDataFrame (
        data=[
            [1, 'Feliza', 'Eusden', 'feusden0@ameblo.jp', 'France'],
            [2, 'Priscilla', 'Le Pine', 'plepine1@biglobe.ne.jp', 'United Kingdom'],
            [3, 'Jaimie', 'Sandes', 'jsandes2@reuters.com', 'Netherlands'],
            [4, 'Nari', 'Dolphin', 'ndolphin3@cbslocal.com', 'United States']
        ],
        schema=['id', 'first_name', 'last_name', 'email', 'country'])

    transformed_df = filtering_df (input_df, ['United Kingdom', 'Netherlands'])
    expected_df = spark.createDataFrame (
        data=[
            [2, 'Priscilla', 'Le Pine', 'plepine1@biglobe.ne.jp', 'United Kingdom'],
            [3, 'Jaimie', 'Sandes', 'jsandes2@reuters.com', 'Netherlands']
        ],
        schema=['id', 'first_name', 'last_name', 'email', 'country'])

    assert_df_equality (transformed_df, expected_df)


def test_df_filetered_data_not_compare ():
    input_df = spark.createDataFrame (
        data=[
            [1, 'Feliza', 'Eusden', 'feusden0@ameblo.jp', 'France'],
            [2, 'Priscilla', 'Le Pine', 'plepine1@biglobe.ne.jp', 'United Kingdom'],
            [3, 'Jaimie', 'Sandes', 'jsandes2@reuters.com', 'Netherlands'],
            [4, 'Nari', 'Dolphin', 'ndolphin3@cbslocal.com', 'United States']
        ],
        schema=['id', 'first_name', 'last_name', 'email', 'country'])

    transformed_df = filtering_df (input_df, ['United Kingdom', 'Netherlands'])
    expected_df = spark.createDataFrame (
        data=[
            [2, 'Priscilla', 'Le Pine', 'plepine1@biglobe.ne.jp', 'United Kingdom'],
            [3, 'Jaimie', 'Sandes', 'jsandes2@reuters.com', 'Netherlands'],
            [4, 'Nari', 'Dolphin', 'ndolphin3@cbslocal.com', 'United States']
        ],
        schema=['id', 'first_name', 'last_name', 'email', 'country'])
    try:
        assert assert_df_equality (transformed_df, expected_df)
    except DataFramesNotEqualError:
        assert True
