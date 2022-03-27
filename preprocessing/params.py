import os
MODEL_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

TEXT_COLUMNS = ['abstract', 'label']
ORDER_KEY = "numberOfSignatures"
PRIMARY_KEY = "petition_id"
ENABLE_LEMMATISATION = True
IGNORE_STOPPING_WORDS = True
TOP_K = 20
MIN_LETTERS = 5
ELE_LOWER_TRANS_OUTPUT_SCHEMA_PATH = os.path.join(MODEL_DIRECTORY,
                                                  'schemas',
                                                  'element_lower_transform_output_schema.json')