import pytest
import pandas as pd
from pandas.testing import assert_series_equal
from preprocessing.word_process import WordCount, WordFrequency


class TestWordCount:
    @pytest.fixture(autouse=True)
    def initialise_word_counter(self):
        self.word_counter = WordCount()
        self.target_words = ['government', 'law']

    @pytest.mark.parametrize(
        ('record', 'columns', 'expected'),
        [
            ({'label': 'this is a test label'},
             ['label'],
             {'label': 'this is a test label',
              'label_length': 5}),
            ({'label': 'this is a test label',
              'abstract': 'this is a test abstract'},
             ['label', 'abstract'],
             {'label': 'this is a test label',
              'abstract': 'this is a test abstract',
              'label_length': 5,
              'abstract_length': 5}),
            (pd.Series({'label': 'this is a new test label',
                        'abstract': 'hehe this is another new test abstract'}),
             ['label', 'abstract'],
             pd.Series({'label': 'this is a new test label',
                        'abstract': 'hehe this is another new test abstract',
                        'label_length': 6,
                        'abstract_length': 7})),
        ]
    )
    def test_get_word_counts(self, record, columns, expected):
        if isinstance(record, dict):
            assert self.word_counter.get_word_counts(record, columns) == expected
        elif isinstance(record, pd.Series):
            assert_series_equal(self.word_counter.get_word_counts(record, columns),
                                expected)

    @pytest.mark.parametrize(
        ('record', 'columns', 'expected'),
        [
            ({'label': 'government government government',
              'abstract': 'government government news law'},
             ['label'],
             pd.Series(
                 {'government': 3,
                  'law': 0}
             )),
            ({'label': 'government government government',
              'abstract': 'government government news law'},
             ['label', 'abstract'],
             pd.Series(
                 {'government': 5,
                  'law': 1}
             )),
            (pd.Series({'label': 'government government come people law',
                        'abstract': 'government government news law'}),
             ['label', 'abstract'],
             pd.Series(
                 {'government': 4,
                  'law': 2}
             )),
        ]
    )
    def test_get_target_counts(self, record, columns, expected):
        assert_series_equal(self.word_counter.get_target_count(record, columns, self.target_words),
                            expected)


class TestWordFrequency:
    @pytest.fixture(autouse=True)
    def initialise_word_frequency(self):
        self.word_frequency = WordFrequency()

    @pytest.mark.parametrize(
        ('record', 'columns', 'expected'),
        [
            ({'label': 'government government come people law',
              'abstract': 'government government news law'},
             ['label'],
             {'government': 2,
              'come': 1,
              'people': 1,
              'law': 1}
             ),
            ({'label': 'government government come people law',
              'abstract': 'government government news law'},
             ['label', 'abstract'],
             {'government': 4,
              'come': 1,
              'people': 1,
              'news': 1,
              'law': 2}
             ),
        ]
    )
    def test_get_word_frequency_one(self, record, columns, expected):
        self.word_frequency.get_word_frequency(record, columns)
        assert self.word_frequency.table == expected

    @pytest.mark.parametrize(
        ('record_list', 'columns', 'expected'),
        [
            ([{'label': 'government government come people law',
              'abstract': 'government government news law'},
              {'label': 'government government come people law',
               'abstract': 'government government news law'}],
             ['label', 'abstract'],
             {'government': 8,
              'come': 2,
              'people': 2,
              'news': 2,
              'law': 4}
             ),
        ]
    )
    def test_get_word_frequency_multiple(self, record_list, columns, expected):
        for record in record_list:
            self.word_frequency.get_word_frequency(record, columns)
        assert self.word_frequency.table == expected

    @pytest.mark.parametrize(
        ('lookup_table', 'top_k', 'min_letters', 'ignore_stopping_words', 'expected'),
        [
            ({'aaaa': 15,
              'bbb': 13,
              'is': 43},
             2,
             2,
             False,
             ['is', 'aaaa']
            ),
            ({'aaaa': 15,
              'bbb': 13,
              'is': 43},
             2,
             3,
             False,
             ['aaaa', 'bbb']
             ),
            ({'aaaa': 15,
              'bbb': 13,
              'is': 43},
             2,
             2,
             True,
             ['aaaa', 'bbb']
             ),
            ({},
             2,
             3,
             False,
             []
             ),
        ]
    )
    def test_get_top_k(self, lookup_table, top_k, min_letters, ignore_stopping_words, expected):
        # initialise lookup table
        self.word_frequency.table = lookup_table
        assert self.word_frequency.get_top_k(top_k, min_letters, ignore_stopping_words) == expected

    @pytest.mark.parametrize(
        ('lookup_table', 'top_k', 'min_letters', 'ignore_stopping_words', 'expected'),
        [
            ({'aaaa': 15,
              'bbb': 13,
              'cc': 43},
             -3,
             2,
             False,
             'top_k and min_letters must be positive integers'
            ),
            ({'aaaa': 15,
              'bbb': 13,
              'cc': 43},
             2,
             -2,
             False,
             'top_k and min_letters must be positive integers'
             ),
            ({'aaaa': 15,
              'bbb': 13,
              'cc': 43},
             None,
             -2,
             False,
             'top_k and min_letters must be positive integers'
             ),
        ]
    )
    def test_get_top_k_error(self, lookup_table, top_k, min_letters, ignore_stopping_words, expected):
        # initialise lookup table
        self.word_frequency.table = lookup_table
        with pytest.raises(ValueError) as e_info:
            self.word_frequency.get_top_k(top_k, min_letters, ignore_stopping_words)
            assert e_info == expected
