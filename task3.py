import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession
import json
import sys
import re
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.types import StringType
from dateutil.parser import parse
from datetime import datetime
from pyspark.sql.window import Window

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

filename="/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz"

log("Started processing - " + filename)

def create_neighbourhoods_dict():
    neighbourhoods_dict = {}
    with open('Neighbourhoods.tsv', 'r') as f:
        for i in f.read().splitlines():
            line = i.split('\t')
            neighbourhoods_dict[line[0]] = line[1].split(', ')
    return neighbourhoods_dict

neighbourhoods_dict = create_neighbourhoods_dict()

def clean_complaint_types(string):
    if re.match('^[a-zA-Z]+$', string):
        return string.lower()
    return ''

borough_list = ['manhattan', 'queens', 'bronx', 'brooklyn', 'staten island', 'new york']
def check_borough(city, borough):
    if city == None:
        if borough != 'Unspecified':
            return borough
        return ''
    if borough == 'Unspecified':
        if city.lower() in borough_list:
            if city.lower() == 'new york':
                return 'MANHATTAN'
            return city
        else:
            for i in neighbourhoods_dict:
                if city.lower() in neighbourhoods_dict[i]:
                    return i.upper()
            return ''
    return borough

def trim_date(datestr):
    if re.match('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}', datestr):
        matched_str = re.search('([0-9]{2})/([0-9]{2})/([0-9]{4}) [0-9]{1,2}:[0-9]{2}', datestr)
        new_datestr = matched_str.group(3) + '/' + matched_str.group(1) + '/' + matched_str.group(2)
        return new_datestr
    return datestr

complaint_type_column_handler = F.udf(lambda x: clean_complaint_types(x), StringType())
borough_handler = F.udf(check_borough, StringType())
trim_date_handler = F.udf(lambda x: trim_date(x), StringType())

input_data = spark.read.format('csv').options(header='true', delimiter='\t').load(filename)
# input_data_columns = ['Unique Key', 'Created Date', 'Closed Date', 'Agency', 'Agency Name', 'Complaint Type', 'Descriptor', 'Location Type', 'Incident Zip', 'Incident Address', 'Street Name', 'Cross Street 1', 'Cross Street 2', 'Intersection Street 1', 'Intersection Street 2', 'Address Type', 'City', 'Landmark', 'Facility Type', 'Status', 'Due Date', 'Resolution Description', 'Resolution Action Updated Date', 'Community Board', 'BBL', 'Borough', 'X Coordinate (State Plane)', 'Y Coordinate (State Plane)', 'Open Data Channel Type', 'Park Facility Name', 'Park Borough', 'Vehicle Type', 'Taxi Company Borough', 'Taxi Pick Up Location', 'Bridge Highway Name', 'Bridge Highway Direction', 'Road Ramp', 'Bridge Highway Segment', 'Latitude', 'Longitude', 'Location']
# Required Columns: 'Unique Key', 'Created Date', 'Closed Date', 'Complaint Type', 'City', 'Borough'

required_data = input_data.select('Unique Key', 'Created Date', 'Closed Date', complaint_type_column_handler(col('Complaint Type')).alias('Complaint Type'), 'City', borough_handler('City', 'Borough').alias('Borough')).orderBy('Complaint Type').dropna().filter(F.col('Complaint Type') != '').filter(F.col('Borough') != '')

# complaint_types_list = ['agency', 'appliance', 'asbestos', 'atf', 'boilers', 'comments', 'construction', 'cst', 'drie', 'drinking', 'electric', 'electrical', 'elevator', 'eviction', 'facades', 'fatf', 'fcst', 'fhe', 'forms', 'general', 'graffiti', 'health', 'heating', 'lead', 'lifeguard', 'linknyc', 'mold', 'mosquitoes', 'msother', 'noise', 'nonconst', 'panhandling', 'plant', 'plumbing', 'question', 'rangehood', 'rodent', 'safety', 'scrie', 'sewer', 'smoking', 'snow', 'snw', 'squeegee', 'srde', 'structural', 'tanning', 'tattooing', 'traffic', 'vending', 'weatherization']
# complaint_types_list = complaint_types.rdd.map(lambda x: x['Complaint Type']).collect()

count_by_borough = required_data.groupBy('Complaint Type', 'Borough').agg(F.count('Complaint Type').alias('count')).orderBy(['Borough', 'count'], ascending=[1,0])
window = Window.partitionBy('Borough').orderBy(F.desc('count'))
count_by_borough_ranked = count_by_borough.select('*', F.rank().over(window).alias('rank')).filter('rank < 4')
count_by_borough_ranked.write.csv("nap493_top3_complaints")
# Top 3 complaints in each borough stored in count_by_borough_ranked

complaints_over_time = required_data.select('Unique Key', trim_date_handler('Created Date').alias('Created Date'), trim_date_handler('Closed Date').alias('Closed Date'), 'Complaint Type', 'Borough')
complaints_over_time_top3 = complaints_over_time.join(count_by_borough_ranked.select('Complaint Type', 'Borough', 'rank'), on=['Complaint Type', 'Borough'])
complaints_over_time_top3 = complaints_over_time_top3.groupBy('Created Date', 'Complaint Type', 'Borough').agg(F.count('*').alias('count'))

complaints_over_time_top3.select('*').orderBy(['Borough', 'Complaint Type', 'Created Date']).write.csv("nap493_distribution_complaints_over_time")

log("Processed dataset - " + filename)
#input_data_with_count = input_data.join(count_by_borough_count, on=["Complaint_Type_311", "Incident_Address_Borough"])

#omplaints_by_date = input_data_with_count.select('Complaint_Number', 'Incident_Address_Borough', 'Complaint_Type_311', 'Date_Received').orderBy(['Incident_Address_Borough', 'Complaint_Type_311', 'Complaint_Number']).dropna()
