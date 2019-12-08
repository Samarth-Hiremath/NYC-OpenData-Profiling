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

def log(msg):
    date_timestamp = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    print(date_timestamp + " INFO: " + str(msg.encode(sys.stdout.encoding, 'ignore').decode()))

def logError(msg):
    date_timestamp = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    print(date_timestamp + " ERROR: " + str(msg.encode(sys.stdout.encoding, 'ignore').decode()))

filename="/user/hm74/NYCOpenData/9jgj-bmct.tsv.gz"

log("Started processing - " + filename)
# filename = "/user/hm74/NYCOpenData/" + filename
input_data = spark.read.format('csv').options(header='true', delimiter='\t').load(filename)

complaint_types = input_data.select('Complaint_Type_311').distinct().dropna().orderBy('Complaint_Type_311')

count_by_borough = input_data.select('Complaint_Number', 'Incident_Address_Borough', 'Complaint_Type_311').dropna()
count_by_borough_count = count_by_borough.groupBy('Incident_Address_Borough', 'Complaint_Type_311').agg(F.count('Complaint_Type_311').alias('count')).orderBy(['Incident_Address_Borough', 'count'], ascending=[1,0])
count_by_borough_count_ranked = count_by_borough_count.select('*', F.rank().over(window).alias('rank')).filter('rank < 4')

input_data_with_count = input_data.join(count_by_borough_count, on=["Complaint_Type_311", "Incident_Address_Borough"])

complaints_by_date = input_data.select('Complaint_Number', 'Incident_Address_Borough', 'Complaint_Type_311', 'Date_Received').orderBy(['Incident_Address_Borough', 'Complaint_Type_311', 'Complaint_Number']).dropna()

