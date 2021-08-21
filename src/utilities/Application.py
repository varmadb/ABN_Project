from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import os
import sys
import logging
import chispa


logging.basicConfig(filename='../../Application.log', level=logging.INFO, filemode='w', format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')

schema_clients = StructType([StructField('id', IntegerType (), True),
                              StructField('first_name', StringType (), True),
                              StructField('last_name', StringType (), True),
                              StructField('email', StringType (), True),
                              StructField('country', StringType (), True)])

financial_clients = StructType([StructField ('id', IntegerType (), True),
                                 StructField ('btc_a', StringType (), True),
                                 StructField ('cc_t', StringType (), True),
                                 StructField ('cc_n', LongType (), True),
                                 ])


def filtering_df(clinets_details):
    """
    This function filter the data based countries from client dataset
    :type clients_details: object
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
    # n = int(sys.argv[1])
    # a = 2
    # tables = []
    # for _ in range (n):
    #     tables.append (sys.argv[a])
    #     a += 1
    # print (tables)

    spark = SparkSession.builder.appName ("ABN_AMRO").master ("local[*]").getOrCreate ()
    sc = spark.sparkContext

    # Client_columns_to_drop = ['first_name', 'last_name']
    # Client_columns_to_drop = ['first_name', 'last_name']
    if os.path.exists ('Source_Dir/dataset_one1.csv'):
        logging.info('dataset_one existis')
    else:
        logging.error('Dataset_one file doesnt exist')

    if os.path.exists ('../../Source_Dir/dataset_two.csv'):
        logging.info('dataset_one existis')
    else:
        logging.error('Dataset_two file doesnt exist')

    clients_df = spark.read \
        .format ("csv") \
        .schema (schema_clients) \
        .option ("header", "true") \
        .load ('Source_Dir/dataset_one.csv')

    logging.info('Client data from created')

    Final_client_data = filtering_df (clients_df)

    logging.info('Client data filtered')

    financial_df = spark.read \
        .format ("csv") \
        .option ("header", "true") \
        .schema (financial_clients) \
        .load ('Source_Dir/dataset_two.csv')
    logging.info('financial data from created')
    Final_financial_df = rename_Column (financial_df)
    logging.info ('financial data fram cloumns renamed')

    Final_client_df = Final_client_data.join (Final_financial_df,
                                              Final_client_data.id == Final_financial_df.client_identifier, "inner") \
        .select ('client_identifier', 'email', 'country', 'bitcoin_address', 'credit_card_type')



    Final_client_df.write.mode ("append") \
        .format("csv") \
        .save ("client_data/", header='true')

    logging.info('Final file craeted in client_data')
