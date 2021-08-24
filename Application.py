################################
# This function filter the data based countries from financial dataset
################################
import logging
import os
import sys
import pandas as pd

from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

logger = logging.getLogger('Application.log')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('Log/Application.log', maxBytes=2000, backupCount=10)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class NoOfArg(Exception):
    """this is for incorrect arguments exception"""
    pass

class Pathdoesnotexist(Exception):
    """this is for incorrect file path"""
    pass

################################
# schema declaration
################################
clients_schema = StructType([StructField('id', IntegerType(), True),
                             StructField('first_name', StringType(), True),
                             StructField('last_name', StringType(), True),
                             StructField('email', StringType(), True),
                             StructField('country', StringType(), True)])

financial_schema = StructType([StructField('id', IntegerType(), True),
                                StructField('btc_a', StringType(), True),
                                StructField('cc_t', StringType(), True),
                                StructField('cc_n', LongType(), True),
                                ])


def filtering_df (clinets_details, country_list):
    """
    This function filter the data based countries from client dataset
    """
    fl_clients = clinets_details.where((col("country").isin(country_list)))
    return fl_clients


def rename_Column (financial_details):
    """
    This function rename 'id', 'btc_a', 'cc_t' as 'client_identifier','bitcoin_address','credit_card_type'
    :type financial_details: object
    """
    try:
        if pd.Series(['id', 'btc_a', 'cc_t']).isin(financial_details.columns).all():
            renamed_df = financial_details.select(col("id").alias("client_identifier"),
                                                  col("btc_a").alias("bitcoin_address"),
                                                  col("cc_t").alias("credit_card_type")
                                                  )
            return renamed_df
        else:
            raise ValueError("Required columns not present in the dataset ")
    except ValueError as exp:
        print('Required columns not present in the dataset ')


if __name__ == '__main__':

    logger.info('Job started')
    try:
        n = len(sys.argv)  # int(sys.argv[1])
        if n > 3:
            logger.info("number of arguments :%s", n - 1)
            dataset_ond_filepath = sys.argv[1]
            dataset_two_filepath = sys.argv[2]
            country_list = str(sys.argv[3]).split(',')
        else:
            raise NoOfArg()

        logger.info(f'dataset one file name : {dataset_ond_filepath}')
        logger.info(f'dataset two file name : {dataset_two_filepath}')
        logger.info(f'dataset country list : {country_list}')

        spark = SparkSession.builder.appName("ABN_AMRO").master("local[*]").getOrCreate()
        sc = spark.sparkContext

        if os.path.exists(dataset_ond_filepath):
            logger.info('dataset_one exists')
        else:
            logger.error('Dataset_one file doesnt exist')
            raise Pathdoesnotexist

        if os.path.exists(dataset_two_filepath):
            logger.info('dataset_two exists')
        else:
            logger.error('Dataset_two file doesnt exist')
            raise Pathdoesnotexist

        clients_df = spark.read \
            .format("csv") \
            .schema(clients_schema) \
            .option("header", "true") \
            .load(dataset_ond_filepath)

        logger.info('Client data from created')

        Final_client_data = filtering_df(clients_df, country_list)

        logger.info('Client data filtered')

        financial_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(financial_schema) \
            .load(dataset_two_filepath)

        logger.info('financial data from created')

        Final_financial_df = rename_Column(financial_df)

        logger.info('financial data farm columns renamed')

        Final_client_df = Final_client_data.join(Final_financial_df,
                                                 Final_client_data.id == Final_financial_df.client_identifier, "inner") \
            .select('client_identifier', 'email', 'country', 'bitcoin_address', 'credit_card_type')

        Final_client_df.write.mode("append") \
            .format("csv") \
            .save('client_data/', header='true')

        logger.info('Final file created in client_data')
        logger.info('Job completed')

    except NoOfArg:
        logger.exception("wrong number of arguments ")
        pass
    except Pathdoesnotexist:
        logger.exception("file path doesnt exist")
        pass
