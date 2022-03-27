import os
import json
from pyspark.sql.types import StructType

MODEL_PATH = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(MODEL_PATH, 'element_lower_transform_output_schema.json')) as f:
    initial_schema = StructType.fromJson(json.load(f))
