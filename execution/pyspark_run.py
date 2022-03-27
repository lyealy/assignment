import os
import argparse
import logging
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import Window, DataFrame as sparkDF
from preprocessing.text_process import TextPreprocessing
from preprocessing.word_process import WordCount, WordFrequency
from preprocessing.params import (
    TEXT_COLUMNS,
    ORDER_KEY,
    PRIMARY_KEY,
    ENABLE_LEMMATISATION,
    IGNORE_STOPPING_WORDS,
    TOP_K,
    MIN_LETTERS,
)
from typing import Iterator, List


formatter = '%(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO,
                    format=formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def element_lower_transform_spark(partition: Iterator) -> Row:
    """
    Pyspark mapping function - call TextPreprocessing operation on RDDs
    """
    text_preprocessor = TextPreprocessing()
    for sample in partition:
        sample_dict = sample.asDict()
        output_sample = text_preprocessor.extract_text(sample_dict, columns=TEXT_COLUMNS,
                                                       enable_lemmatisation=ENABLE_LEMMATISATION)
        yield Row(**output_sample)


def element_count_transform_spark(partition: Iterator) -> Row:
    """
    Pyspark mapping function - call WordCount operation on RDDs
    """
    for sample in partition:
        sample_dict = sample.asDict()
        output_sample = WordCount.get_word_counts(sample_dict, columns=TEXT_COLUMNS)
        yield Row(**output_sample)


def element_target_count_transform_spark(partition: Iterator, target_words: List[str]) -> Row:
    """
    Pyspark mapping function - call WordCount operation on RDDs
    """
    for sample in partition:
        sample_dict = sample.asDict()
        output_sample = WordCount.get_target_count(sample_dict, columns=TEXT_COLUMNS,
                                                   target_words=target_words).to_dict()
        output_sample[PRIMARY_KEY] = sample_dict[PRIMARY_KEY]
        yield Row(**output_sample)


def generate_lookup_table(spark_df: sparkDF, TEXT_COLUMNS: List[str]) -> sparkDF:
    """
    Generate lookup table: word -> count.
    Parameters
    ----------
    spark_df:
        A pyspark dataframe contains one or multiple text columns.
    TEXT_COLUMNS
        A list of selected text columns.
    Returns
    -------
        A pyspark dataframe contains word and count columns.
    """

    concate_columns = []
    for i, column in enumerate(TEXT_COLUMNS):
        if i > 0:
            concate_columns.append(F.lit(' '))
        concate_columns.append(F.col(column))

    full_text_df = spark_df.withColumn('full_text', F.concat(*concate_columns)).select('full_text')
    full_text_split_df = full_text_df.select(F.split(full_text_df.full_text, '\s+').alias('split'))
    full_text_single_df = full_text_split_df.select(F.explode(full_text_split_df.split).alias('word'))
    words_df = full_text_single_df.where(full_text_single_df.word != '')
    words_df = words_df.groupBy('word').agg(F.count('word').alias('count'))
    return words_df


def run_etl(input_fpath: str, artefacts_path: str):
    """
    Run etl job.
    Parameters
    ----------
    input_fpath:
        Input file path.
    artefacts_path:
        Output path where to store output artefacts.
    """

    spark = SparkSession.builder.appName('WordCount').getOrCreate()

    # Step 1 - Load raw data
    spark_df = spark.read.json(input_fpath)
    logger.info(f'Load raw data from {input_fpath}. Raw dataset contains {spark_df.count()} records.')

    # Step 2 - Extract text from nested data structure and basic preprocessing
    logger.info('Extract text from raw data with preprocessing...')
    logger.info(f'Enable lemmatisation: {ENABLE_LEMMATISATION}')
    spark_df = spark_df.rdd.mapPartitions(lambda x: element_lower_transform_spark(x)).toDF()

    # Step 3 - Combine duplicated petitions
    logger.info('Combine records with same text information...')
    spark_df = spark_df.groupBy(TEXT_COLUMNS).agg(F.sum(ORDER_KEY).alias(ORDER_KEY))
    logger.info(f'New dataset contains {spark_df.count()} records')

    # Step 4 - Get abstract and label lengths
    spark_df = spark_df.rdd.mapPartitions(lambda x: element_count_transform_spark(x)).toDF()
    spark_df = spark_df.orderBy(F.col(ORDER_KEY).desc())
    spark_df = spark_df.withColumn(PRIMARY_KEY, F.row_number().over(
        Window.orderBy(F.monotonically_increasing_id()))-1).cache()
    selected_columns = [PRIMARY_KEY]+[feature + '_length' for feature in TEXT_COLUMNS] + [ORDER_KEY]
    output_fpath = os.path.join(artefacts_path, 'output1')
    spark_df.select(selected_columns).write.csv(output_fpath, header=True, mode='overwrite')
    logger.info(f'Save output1 to {output_fpath}')

    # Step 5 - Build lookup table
    logger.info('Building lookup table...')
    words_df = generate_lookup_table(spark_df, TEXT_COLUMNS).toPandas()
    lookup_table = {}
    for item in words_df.to_dict('records'):
        lookup_table[item['word']] = item['count']
    word_frequency = WordFrequency()
    word_frequency.table = lookup_table
    logger.info('Saving word frequency lookup table')
    word_frequency.save_table(fpath=os.path.join(artefacts_path, 'table.pkl'))

    # Step 6 - Get top_k common words
    logger.info(f'Get top {TOP_K} common words.')
    logger.info(f'Ignore stopping words: {IGNORE_STOPPING_WORDS}')
    target_words = word_frequency.get_top_k(TOP_K, MIN_LETTERS, ignore_stopping_words=IGNORE_STOPPING_WORDS)
    spark_df = spark_df.rdd.mapPartitions(lambda x: element_target_count_transform_spark(x, target_words)).toDF()
    output_fpath = os.path.join(artefacts_path, 'output2')
    spark_df.select([PRIMARY_KEY] + target_words).write.csv(output_fpath, header=True, mode='overwrite')
    logger.info(f'Save output2 to {output_fpath}')

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_fpath', required=True, default=None, type=str, help='Input file path.')
    parser.add_argument('--artefacts_path', required=True, default=None, type=str, help='Output path to store artefacts.')
    args = parser.parse_args()
    input_fpath = args.input_fpath
    artefacts_path = args.artefacts_path
    run_etl(input_fpath=input_fpath,
            artefacts_path=artefacts_path)
