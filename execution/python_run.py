import pandas as pd
import swifter
import os
import logging
import argparse
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

formatter = '%(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO,
                    format=formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run_etl(input_fpath: str,
            artefacts_path:str,
            ) -> None:
    """
    Run etl job.
    Parameters
    ----------
    input_fpath:
        Input file path.
    artefacts_path:
        Output path where to store output artefacts.
    """

    # Step 1 - Load raw data
    df = pd.read_json(input_fpath, orient='records')
    logger.info(f'Load raw data from {input_fpath}. Raw dataset contains {len(df)} records.')

    # Step 2 - Extract text from nested data structure and basic preprocessing
    logger.info('Extract text from raw data with preprocessing...')
    logger.info(f'Enable lemmatisation: {ENABLE_LEMMATISATION}')
    text_preprocessor = TextPreprocessing()
    df = df.swifter.apply(text_preprocessor.extract_text,
                          columns=TEXT_COLUMNS,
                          enable_lemmatisation=ENABLE_LEMMATISATION,
                          axis=1)

    # Step 3 - Combine duplicated petitions
    logger.info('Combine records with same text information...')
    df = df.groupby(TEXT_COLUMNS).agg('sum').reset_index()
    logger.info(f'New dataset contains {len(df)} records')

    # Step 4 - Get abstract and label lengths
    df = df.swifter.apply(WordCount.get_word_counts, columns=TEXT_COLUMNS, axis=1)
    df = df.sort_values(by=ORDER_KEY, ascending=False)
    df = df.reset_index(drop=True)
    df.index.name = PRIMARY_KEY

    selected_columns = [feature + '_length' for feature in TEXT_COLUMNS] + [ORDER_KEY]
    output_fpath = os.path.join(artefacts_path, 'output1.csv')
    df[selected_columns].to_csv(output_fpath)
    logger.info(f'Save output1 to {output_fpath}')

    # Step 5 - Build lookup table
    logger.info('Building lookup table...')
    word_frequency = WordFrequency()
    _ = df.apply(word_frequency.get_word_frequency,
                 columns=TEXT_COLUMNS,
                 axis=1)
    logger.info('Saving word frequency lookup table')
    word_frequency.save_table(fpath=os.path.join(artefacts_path, 'table.pkl'))

    # Step 6 - Get top_k common words
    logger.info(f'Get top {TOP_K} common words.')
    logger.info(f'Ignore stopping words: {IGNORE_STOPPING_WORDS}')
    target_words = word_frequency.get_top_k(top_k=TOP_K,
                                            min_letters=MIN_LETTERS,
                                            ignore_stopping_words=IGNORE_STOPPING_WORDS)
    output2_df = df.swifter.apply(WordCount.get_target_count,
                                  columns=TEXT_COLUMNS,
                                  target_words=target_words,
                                  axis=1)
    output_fpath = os.path.join(artefacts_path, 'output2.csv')
    output2_df.to_csv(output_fpath)
    logger.info(f'Save output2 to {output_fpath}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_fpath', required=True, default=None, type=str, help='Input file path.')
    parser.add_argument('--artefacts_path', required=True, default=None, type=str, help='Output path to store artefacts.')
    args = parser.parse_args()
    input_fpath = args.input_fpath
    artefacts_path = args.artefacts_path
    run_etl(input_fpath=input_fpath,
            artefacts_path=artefacts_path)
