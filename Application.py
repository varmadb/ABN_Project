from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import os
import sys
import logging


class NoOfArg (Exception):
    """Base class for other exceptions"""
    pass


logging.basicConfig (filename="Application.log", level=logging.INFO, filemode='w',
                     format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')

schema_clients = StructType ([StructField ('id', IntegerType (), True),
                              StructField ('first_name', StringType (), True),
                              StructField ('last_name', StringType (), True),
                              StructField ('email', StringType (), True),
                              StructField ('country', StringType (), True)])

financial_clients = StructType ([StructField ('id', IntegerType (), True),
                                 StructField ('btc_a', StringType (), True),
                                 StructField ('cc_t', StringType (), True),
                                 StructField ('cc_n', LongType (), True),
                                 ])


def filtering_df (clinets_details):
    """
    This function filter the data based countries from client dataset
    """
    fl_clients = clinets_details.filter (
        (clinets_details.country == "Netherlands") | (clinets_details.country == "United Kingdom")) \
        .select ('id', 'email', 'country')
    return fl_clients


def rename_Column (financial_details):
    """
    This function filter the data based countries from financial dataset
    :type financial_details: object
    """
    renamed_df = financial_details.select (col ("id").alias ("client_identifier"),
                                           col ("btc_a").alias ("bitcoin_address"),
                                           col ("cc_t").alias ("credit_card_type")
                                           )
    return renamed_df


if __name__ == '__main__':
    try:
        n = len (sys.argv)  # int(sys.argv[1])
        if n > 3:
            logging.info ("number of arguments :%s", n - 1)
            dataset_ond_filepath = sys.argv[1]
            dataset_two_filepath = sys.argv[2]
            country_list = str (sys.argv[3]).split (',')
        else:
            raise NoOfArg

        print (len (sys.argv))
        print (f'dataset one file name : {dataset_ond_filepath}')
        print (f'dataset two file name : {dataset_two_filepath}')
        print (f'dataset country list : {country_list}')

        spark = SparkSession.builder.appName ("ABN_AMRO").master ("local[*]").getOrCreate ()
        sc = spark.sparkContext

        if os.path.exists (dataset_ond_filepath):
            logging.info ('dataset_one exists')
        else:
            logging.error ('Dataset_one file doesnt exist')

        if os.path.exists (dataset_two_filepath):
            logging.info ('dataset_one exists')
        else:
            logging.error ('Dataset_two file doesnt exist')

        clients_df = spark.read \
            .format ("csv") \
            .schema (schema_clients) \
            .option ("header", "true") \
            .load (dataset_ond_filepath)
        # .load ('Source_Dir/dataset_one.csv')

        logging.info ('Client data from created')

        Final_client_data = filtering_df (clients_df)

        logging.info ('Client data filtered')

        financial_df = spark.read \
            .format ("csv") \
            .option ("header", "true") \
            .schema (financial_clients) \
            .load (dataset_two_filepath)
        # .load ('Source_Dir/dataset_two.csv')

        logging.info ('financial data from created')

        Final_financial_df = rename_Column (financial_df)

        logging.info ('financial data farm columns renamed')

        Final_client_df = Final_client_data.join (Final_financial_df,
                                                  Final_client_data.id == Final_financial_df.client_identifier, "inner") \
            .select ('client_identifier', 'email', 'country', 'bitcoin_address', 'credit_card_type')

        Final_client_df.write.mode ("append") \
            .format ("csv") \
            .save ("client_data/", header='true')

        logging.info ('Final file created in client_data')

    except NoOfArg:
        logging.exception ("wrong number of arguments ")
