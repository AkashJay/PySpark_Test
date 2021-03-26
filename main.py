import shared.services as shared_services
import sys
from pyspark.sql import DataFrame

if __name__ == '__main__':
    # Create Logger object
    logger = shared_services.get_logger()
    logger.info("Application Started..")

    # Create Spark Session
    ss = shared_services.get_spark_session()

    # Create Spark Context
    sc = shared_services.get_spark_context()

    # Getting input and output file paths from arguments
    file_path_sl = sys.argv[1]
    file_path_in = sys.argv[2]
    output_file_path = sys.argv[3]

    # Read input data from source files
    logger.info("def read_text_files_to_rdd invoked")
    rdd_sl = shared_services.read_text_files_to_rdd(sc, file_path_sl)
    rdd_in = shared_services.read_text_files_to_rdd(sc, file_path_in)

    # Convert RDD into a Dataframe
    logger.info("def rdd_to_df invoked")
    schema = ["train_name", "d1", "d2", "status"]
    df_sl: DataFrame = shared_services.rdd_to_df(rdd_sl, schema)
    df_in: DataFrame = shared_services.rdd_to_df(rdd_in, schema)

    logger.info("def rdd_to_df invoked")
    exceptDF = shared_services.extract_dataframes(df_in, df_sl, "alias1", "alias2", "status")
    exceptDF.printSchema()
    exceptDF.show()

    # Union dataframes
    dfs = {df_sl, df_in}
    union_df = shared_services.union_all(*dfs)

    # concat dataframes columns with ";" delimiter to a single column
    df_text = union_df \
        .withColumn("combined", shared_services.concat_df_columns(*union_df.columns)).select("combined")

    # Print  dataframe in console for debugging
    df_text.show()

    # Sava output to text file
    df_text.coalesce(1) \
        .write.format("text") \
        .option("header", "false") \
        .mode("append") \
        .save(output_file_path)

    logger.info("Application Completed")
