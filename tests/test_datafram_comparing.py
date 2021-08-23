from chispa.dataframe_comparer import *
from Application import rename_Column
from tests.spark import spark


def test_df_compare_schema_equal ():
    input_df = spark.createDataFrame (
        data=[[1, '1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2', 'visa - electro1', 4175006996999270],
              [2, '1wjtPamAZeGhRnZfhBA4324jNvnHefd2V2', 'visa - electro2', 4175006996999271],
              [3, '1wjtPamAZeGhRnZfhBA4324jNvnHefd2V2', 'visa - electro2', 4175006996999272]],
        schema=['id', 'btc_a', 'cc_t', 'cc_n'])

    transformed_df = rename_Column (input_df)

    expected_df = spark.createDataFrame (
        data=[[1, '1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2', 'visa - electro1'],
              [2, '1wjtPamAZeGhRnZfhBA4324jNvnHefd2V2', 'visa - electro2'],
              [3, '1wjtPamAZeGhRnZfhBA4324jNvnHefd2V2', 'visa - electro2']],
        schema=['client_identifier', 'bitcoin_address', 'credit_card_type'])

    assert_df_equality (transformed_df, expected_df)


def test_df_compare_schema_Not_equal ():
    input_df = spark.createDataFrame (
        data=[[1, '1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2', 'visa - electro1', 4175006996999270],
              [2, '1wjtPamAZeGhRnZfhBA4324jNvnHefd2V2', 'visa - electro2', 4175006996999271],
              [3, '1wjtPamAZeGhRnZfhBA4324jNvnHefd2V2', 'visa - electro2', 4175006996999272]],
        schema=['id', 'btc_a', 'cc_t', 'cc_n'])

    transformed_df = rename_Column (input_df)

    expected_df = spark.createDataFrame (
        data=[[1, '1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2', 'visa - electro1'],
              [2, '1wjtPamAZeGhRnZfhBA4324jNvnHefd2V2', 'visa - electro2'],
              [3, '1wjtPamAZeGhRnZfhBA4324jNvnHefd2V2', 'visa - electro2']],
        schema=['client_identifier', 'bitcoin_address', 'credit_card_type1'])

    assert_df_equality (transformed_df, expected_df)
