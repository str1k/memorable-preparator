from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import datetime
from pyspark.sql.functions import first
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from operator import add
from functools import reduce

scoring_day = getArgument("scoring_day", "'2019-04-20'")

# Get max date from table on selected date column. 
# return date in string format of YYYY-MM-DD
def getMaxDate(table_name,date_column,is_qouted=True):
  scoring_day = spark.sql("SELECT " + date_column + " FROM " + table_name + " GROUP BY " + date_column + " ORDER BY " + date_column + " DESC LIMIT 1").collect()[0][date_column]
  if type(scoring_day) is datetime.date:
    scoring_day = scoring_day.strftime("%Y-%m-%d")
  if is_qouted:
    scoring_day = "'" + scoring_day + "'"
  return scoring_day

# Select Dataframe from a table on specific date range.
# Return Spark dataframe
def selectRangeDataFrame(table_name,date_column,scoring_day,n_range=90):
  return spark.sql("SELECT * FROM " + table_name + " WHERE date(" + date_column + ") > date_sub(date(" + scoring_day + ")," + str(n_range) + ") AND date(" + date_column + ") <= date(" + scoring_day + ")")

def buildAggDict(sdf,agg_func):
  agg_dict = {}
  for s in sdf.select([c for c in sdf.columns if c not in {'day_id','analytic_id','register_date','crm_subscription_id','ddate'}]).columns:
    agg_dict[s] = agg_func
  return agg_dict

# Pivot reccord-based Spark dataframe group by two keys on a specified column.
# Return Spark dataframe
def getPivotedDataFrame(sdf,key_1,key_2,pivot_column):
  agg_dict = buildAggDict(sdf,'first')
  return sdf.select([c for c in sdf.columns if c not in {'day_id','crm_subscription_id'}]).groupby(col(key_1), col(key_2)).pivot(pivot_column).agg(agg_dict)

def buildPivotedColumnNameList(sdf,key_1,key_2,n_range):
  column_list = []
  for s in sdf.columns:
    if s != 'analytic_id' and s != 'register_date':
      if not column_list:
        column_list.append(s.split(',')[0].split('(')[1])
      elif column_list[0] == s.split(',')[0].split('(')[1]:
        break
      else:
        column_list.append(s.split(',')[0].split('(')[1])
  new_column_list = []
  new_column_list.append(key_1)
  new_column_list.append(key_2)
  for i in range(n_range):  
    for s in column_list:
      new_column_list.append(s+"_"+"tm"+str(n_range-(i+1)))
  return column_list,new_column_list

def buildDailySourceFeature(table_name,date_column,pivot_column,key_1,key_2,n_range=90):
  scoring_day = getMaxDate(table_name,date_column)
  print(scoring_day)
  sdf = selectRangeDataFrame(table_name,date_column,scoring_day,n_range)
  pivoted_sdf = getPivotedDataFrame(sdf,key_1,key_2,pivot_column)
  column_list,new_names = buildPivotedColumnNameList(pivoted_sdf,key_1,key_2,n_range)
  pivoted_sdf = pivoted_sdf.toDF(*new_names)
  return pivoted_sdf,column_list,scoring_day

#Create List for generating periodic sum using sliding window method
#This function take spark dataframe, indexed list of the first numeric feature, original ordered numberic feature column list,
#stride size, window size, n_range; number of data point, starting_period; for reuse/additional output naming convenient
#output features are created (according to sizing of datapoint-window_size)/stride_size + 1; the formula is created to handle full range of data point
def slidingWindowedSumGenerator(self, indexer_list, column_list, stride_size=3, window_size=7, n_range=90, start_period=0):
  num_iterate = int( (n_range-window_size)/stride_size ) + 1
  output_feature_list = []
  for i in range (0,len(column_list)):
    for j in range (0, num_iterate):
      output_feature_list.append(["sum_window_"+ column_list[i] + "_pm" + str(j+start_period),list( col(self.columns[ci+i]) for ci in indexer_list[(j*stride_size):window_size+(j*stride_size)] )])
  return output_feature_list
#Build aggregate sum feature from list 
def buildSumFeature(self,output_feature_list):
  self = self.na.fill(0).select(['*'] + [sum(l[1]).alias(l[0]) for l in output_feature_list])
  return self

table_name = "mck_raw.dm15_mobile_usage_aggr_prepaid"
date_column = "ddate"
pivot_column = "ddate"
key_1 = "analytic_id"
key_2 = "register_date"
feature_range = 90

output,column_list,scoring_day = buildDailySourceFeature(table_name,date_column,pivot_column,key_1,key_2,feature_range)
output.persist()
output_feature_list = slidingWindowedSumGenerator(output,indexer_list,column_list)
final = buildSumFeature(output,output_feature_list)
final.explain(extended=True)