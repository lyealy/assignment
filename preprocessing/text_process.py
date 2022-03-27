import pandas as pd
import re
from nltk.stem import WordNetLemmatizer
from typing import Union, List, Dict


class TextPreprocessing:
    def __init__(self):
        self.lemmatizer = WordNetLemmatizer()

    def extract_text(self, record: Union[Dict, pd.Series],
                     columns: List[str],
                     enable_lemmatisation=False) -> Union[Dict, pd.Series]:
        """
        Extract texts from nested data structure which simplifies the downstream processing (e.g. remove duplicated
        records).
        Parameters
        ----------
        record:
            A record contains nested data structure.
            e.g.
                "abstract":{
                    "_value": "this is an abstract text"
                },
                "label": {
                    "_value": "this is a label text"
                },
                "numberOfSignatures": 123
        columns:
            A list of column names which contain nested data structures.
        enable_lemmatisation:
            If true, apply lemmatisation before word counting.
            Lemmatisation converts word to its base form:
            e.g. walk, walked, walks or walking -> walk.
        Returns
        -------
            A new record without nested data structure.
            e.g.
                "abstract": "this is an abstract text"
                "label": "this is a label text"
                "numberOfSignatures": 123
        """
        for feature in columns:
            record[feature] = record[feature]['_value']
            record[feature] = self.transform(record[feature], enable_lemmatisation=enable_lemmatisation)
        return record

    def transform(self, text: str, enable_lemmatisation=False) -> str:
        """
        A set of text transforms/preprocessing.
        Note, the transforms listed in this function is just an example. Different downstream tasks may require
        different types of transforms.
        Parameters
        ----------
        text:
            A string contains one or multiple words.
        enable_lemmatisation:
            If true, apply lemmatisation before word counting.
            Lemmatisation converts word to its base form:
            e.g. walk, walked, walks or walking -> walk.
        Returns
        -------
            A preprocessed string.
        """
        # this usually helps avoid duplicates
        text = text.lower()
        # use '[^a-z ]' if treat numbers as invalid words
        text = re.sub(r'[^a-z0-9 ]', '', text)  # remove punctuation
        text = re.sub(' +', ' ', text)  # remove multiple white spaces
        text = text.strip()  # remove leading and trailing spaces

        if enable_lemmatisation:
            word_list = []
            for word in text.split():
                word = self.lemmatizer.lemmatize(word)
                word_list.append(word)
            text = ' '.join(word_list)

        return text
