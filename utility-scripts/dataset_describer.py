import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession
import json
import sys
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
from dateutil.parser import parse
from datetime import datetime

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("hw3") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()


with open('dataset_names_new.txt', 'r') as f:
    dataset_names = f.read().split(", ")

output_list = []
for dataset_name in dataset_names:
    output_list.append(dataset_name)
    input_data = spark.read.format('csv').options(header='true', delimiter='\t').load(dataset_name)
    output_list.append(input_data.columns)
    output_list.append(input_data.count())
    output_list.append("")

output_list_str = "\n".join(str(item) for item in output_list)
output_list_str_decoded = str(output_list_str.encode(sys.stdout.encoding, 'ignore').decode('utf-8', 'ignore'))
with open('columns_in_dataset.txt', 'w') as f:
    f.write(output_list_str_decoded)