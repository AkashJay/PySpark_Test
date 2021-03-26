from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from functools import reduce
import logging
from pyspark.sql.functions import col


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


def extract_dataframes(df1, df2, alias1, alias2, drop_column):
    """
    Method to get the difference of two dataframes without considering a one column
    :param df1: Dataframe that contain the column that should be in final dataframe
    :param df2: Second Dataframe
    :param alias1:  Alias for the dataframe 1
    :param alias2:  Alias for the dataframe 2
    :param drop_column: The column name we that need to be dropped from two dataframes that need to extract
    :return:
    """
    df11 = df1.drop(col(drop_column))
    df22 = df2.drop(col(drop_column))
    except_df = df11.exceptAll(df22)

    columns = [alias1 + '.train_name', alias1 + '.d1', alias1 + '.d2', alias2 + '.status']
    new_df = except_df.alias(alias1).join(df1.alias(alias2),
                                          col(alias1 + ".train_name") == col(alias2 + ".train_name"),
                                          "leftouter").select(columns)

    return new_df

#     df1 = df1.drop(col(drop_column))
#     df2 = df2.drop(col(drop_column))
#     except_df = df1.exceptAll(df2)
#     except_df.show()
#
#     columns = [alias1+'.train_name', 'extract_df.d1', 'extract_df.d2', 'status']
#     final_df = except_df.alias(alias1).join(df2.alias(alias2), col(alias1+".train_name") == col(alias2+".train_name"),
#                                      "leftouter").select(columns)
#     return final_df
