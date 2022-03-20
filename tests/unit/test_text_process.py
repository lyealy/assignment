import pytest
import pandas as pd
from pandas.testing import assert_series_equal
from preprocessing.text_process import TextPreprocessing


class TestTextProcess:
    @pytest.fixture(autouse=True)
    def initialise_text_preprocessor(self):
        self.text_preprocessor = TextPreprocessing()

    @pytest.mark.parametrize(
        ('text', 'enable_lemmatisation', 'expected'),
        [
            ('  This   is A 2349 test ', False, 'this is a 2349 test'),
            (' This IS another test, we have two tests', False, 'this is another test we have two tests'),
            ('We (have) 3^&!     tests ', True, 'we have 3 test')
        ]

    )
    def test_transform(self, text, enable_lemmatisation, expected):
        assert self.text_preprocessor.transform(text, enable_lemmatisation=enable_lemmatisation) == expected

    @pytest.mark.parametrize(
        ('record',
         'columns',
         'enable_lemmatisation',
         'expected'),
        [
            ({'label': {'_value': 'This is a title'}},
             ['label'],
             True,
             {'label': 'this is a title'}),
            ({'label': {'_value': ' This is &* a   title '},
              'abstract': {'_value': 'There are two children.'}},
             ['label', 'abstract'],
             True,
             {'label': 'this is a title',
              'abstract': 'there are two child'}),
            (pd.Series({'label': {'_value': 'This is a title!'}}),
             ['label'],
             True,
             pd.Series({'label': 'this is a title'}))
        ]
    )
    def test_extract_text(self, record, columns, enable_lemmatisation, expected):
        if isinstance(record, dict):
            assert self.text_preprocessor.extract_text(record, columns, enable_lemmatisation) == expected
        elif isinstance(record, pd.Series):
            assert_series_equal(self.text_preprocessor.extract_text(record, columns, enable_lemmatisation),
                                       expected)
