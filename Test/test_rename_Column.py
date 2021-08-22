from Application import rename_Column
from .spark import *
import collections
from chispa import *
# from chispa.dataframe_comparer import are_dfs_equal
# from chispa.schema_comparer import SchemasNotEqualError


def test_rename_Column ():
    source_data = [
        (1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa - electron", "4175006996999270"),
        (2, "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb", "3587679584356527")
    ]
    source_df = spark.createDataFrame (source_data, ["id", "btc_a", "cc_t", "cc_n"])

    renamed_df = rename_Column (source_data)
    expected_data = [
        (1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa - electron", "4175006996999270"),
        (2, "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb", "3587679584356527")
    ]
    expected_df = spark.createDataFrame (renamed_df,
                                         ["client_identifier", "bitcoin_address", "credit_card_type", "cc_n"])
    assert expected_df.column == renamed_df.column