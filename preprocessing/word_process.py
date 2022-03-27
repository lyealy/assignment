import pandas as pd
import heapq
import pickle
import nltk
import logging
from nltk.corpus import stopwords
from typing import Union, List, Dict
from collections import defaultdict, Counter
nltk.download('stopwords')

logger = logging.getLogger(__name__)


class WordCount:
    @classmethod
    def get_word_counts(cls, record: Union[Dict, pd.Series], columns: List[str]) -> Union[Dict, pd.Series]:
        """
        Count words in the selected text objects.
        Parameters
        ----------
        record:
            A record contains one or more text objects.
        columns:
            A list of names of selected text objects.
        Returns
        -------
            A new record which contains the lengths of text objects.

        """
        for feature in columns:
            record[feature + '_length'] = cls.word_count(record[feature])
        return record

    @staticmethod
    def word_count(text: str) -> int:
        """
        Count words in the given text object.
        Parameters
        ----------
        text:
            A string contains one or multiple words.
        Returns
        -------
            Word counts (length) of the text object.
        """
        return len(text.split(' '))

    @staticmethod
    def get_target_count(record: Union[Dict, pd.Series], columns: List[str], target_words: List[str]) -> pd.Series:
        """
        Count target words in the selected columns.
        Parameters
        ----------
        record:
            A record contains one or more text objects.
        columns:
            A list of names of selected text objects.
        target_words:
            A list of interested words.
        Returns
        -------

        """
        res = {k: 0 for k in target_words}
        for feature in columns:
            counts = Counter(record[feature].split())
            for word in target_words:
                if word in counts:
                    res[word] += counts[word]
        return pd.Series(res)


class WordFrequency:
    def __init__(self):
        # Initialize a lookup table and a lemmatizer operator
        self.table = defaultdict(int)

    def get_word_frequency(self, record: Union[Dict, pd.Series], columns: List[str]) -> None:
        """
        Calculate word frequency.
        Parameters
        ----------
        record:
            A record contains one or more text objects.
        columns:
            A list of names of selected text objects.
        Returns
        -------

        """
        for feature in columns:
            counts = Counter(record[feature].split())
            for word, count in counts.items():
                self.table[word] += count

    def save_table(self, fpath: str) -> None:
        """
        Save the lookup table. Avoid multiple calculations.
        Parameters
        ----------
        fpath:
            The path where to save the lookup table.
        """
        logger.info(f'Save lookup table to {fpath}')
        with open(fpath, 'wb') as raw:
            pickle.dump(self.table, raw)

    def load_table(self, fpath: str) -> None:
        """
        Load the lookup table. Avoid multiple calculations.
        Parameters
        ----------
        fpath:
            The path where to load the lookup table.
        """
        logger.info(f'Load lookup table from {fpath}')
        with open(fpath, 'rb') as raw:
            self.table = pickle.load(raw)

    def get_table(self) -> Dict[str, int]:
        """
        Return the lookup table.
        Returns
        -------
            Return the lookup table.
        """
        return self.table

    def get_top_k(self, top_k: int, min_letters=5, ignore_stopping_words: bool = False) -> List[str]:
        """
        Get the top k most common words.
        Parameters
        ----------
        top_k:
            Define the return number of most common words.
        min_letters:
            A threshold. Ignore words with number of letters smaller than min_letters.
        ignore_stopping_words:
            If true, ignore some common words that are frequently used but without much specific information.
            e.g. you're, yourself, itself, having, between, don't ,etc.
        Returns
        -------
            A list of the top k most common words sorted in descending order.

        """
        if top_k is None or top_k < 0 or min_letters < 0:
            logger.error('top_k and min_letters must be positive integers')
            raise ValueError('top_k and min_letters must be positive integers')

        res = []
        if len(self.table) == 0:
            logger.warning('Look up table is empty, please calculate word frequency first.')
            return []

        for word, count in self.table.items():
            # If length is smaller than threshold, ignore this word
            if len(word) < min_letters:
                continue

            # Ignore if the word is a stopword.
            if ignore_stopping_words and (word in stopwords.words('english')):
                continue

            # Get the top_k most common words by using heap, which is faster than 'sort and select'.
            # time complexity: O(nlogk) where n is the total word number and k is the number of required common words
            # space complexity: O(k)
            if len(res) < top_k:
                heapq.heappush(res, (count, word))
            else:
                heapq.heappushpop(res, (count, word))
        return [word for _, word in sorted(res, reverse=True)]
