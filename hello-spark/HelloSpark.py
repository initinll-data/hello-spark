from lib.logger import Log4j
from lib.utils import count_by_country, get_spark_app_config, load_survey_df
from pyspark.sql import *

if __name__ == "__main__":
    conf = get_spark_app_config()
    
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    filepath = "data/sample.csv"

    logger.info("Starting HelloSpark")

    survey_df = load_survey_df(spark, filepath)
    partitioned_survey_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)

    count = count_df.collect()

    logger.info(count)
    logger.info("Finished HelloSpark")

    input("Press Enter")

    spark.stop()
