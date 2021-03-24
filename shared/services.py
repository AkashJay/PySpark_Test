from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from functools import reduce
import logging


def get_spark_session():
    """
    Method to create spark session
    :return: SparkSession
    """
    spark = SparkSession \
        .builder.appName("Pyspark Demo") \
        .master("local[*]") \
        .getOrCreate()
    return spark


def get_spark_context():
    """
    Method to create spark context
    :return: SparkContext
    """
    sc = get_spark_session().sparkContext
    return sc


def read_text_files_to_rdd(sc, file_path):
    """
    Method to read text files to rdd
    :param sc: Spark Context
    :param file_path: Input file path
    :return: Object (RDD[Str])
    """
    rdd = sc.textFile(file_path)
    return rdd


def rdd_to_df(rdd, schema):
    """
    Method to convert rdd to dataframe
    :param rdd: input rdd
    :param schema: dataframe schema
    :return: DataFrame
    """
    df = rdd.map(lambda k: k.split(";")).toDF(schema)
    return df


def union_all(*dfs):
    """
    Method to union multiple dataframes
    :param dfs: Set of dataframes
    :return: DataFrame
    """
    return reduce(DataFrame.union, dfs)


def concat_df_columns(*cols):
    """
    Method to concat all columns into a single column with a delimiter
    :param cols: dataframe columns
    :return: dataframe
    """
    delimiter = ";"
    concat_columns = []
    for c in cols[:-1]:
        concat_columns.append(f.coalesce(c, f.lit("*")))
        concat_columns.append(f.lit(delimiter))
    concat_columns.append(f.coalesce(cols[-1], f.lit("*")))
    return f.concat(*concat_columns)


def get_logger():
    """
    Method to create logger object
    :return: Logger Object
    """
    logger = logging.getLogger('py4j')
    return logger
