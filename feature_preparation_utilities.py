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
from datetime import datetime
from dateutil.relativedelta import relativedelta

# parameter
table_name = "mck_raw.dm1111_rda_revenue_by_service"
date_column = "month_id"
pivot_column = "month_id"
key_1 = "analytic_id"
key_2 = "register_date"

multiple_month_flag = True # for multiple date batch of data
num_month = 4

scoring_day = None # target date, if set 'None' it will search for the max date
  
mode = 'monthly'     # type of aggregate data, can be either 'monthly' or 'daily'
feature_range = 7    # the range of months or days of the data we want to implement into the feature
window_size = 3      # the range of window size
stride_size = 2      # how far the window size slide each time
start_period = 0
num_iterate = 1 if stride_size == 0 else int( (feature_range-window_size)/stride_size ) + 1

if not (mode == "monthly" or mode == "daily"):
  raise Exception("'mode' can only be either 'monthly' or 'daily'")
  
elif feature_range < 2:
  raise Exception("'feature_range' can only be more than 1")
  
elif stride_size < 0:
  raise Exception("'stride_size' can not be negative")

elif window_size < 2:
  raise Exception("'window_size' can only be more than 1")
  


#scoring_day = getArgument("scoring_day", "'2019-04-20'")

# Get max date from table on selected date column. 
# return date in string format of YYYY-MM-DD
def getMaxDate(table_name,date_column,is_qouted=True):
  scoring_day = spark.sql("SELECT " + date_column + " FROM " + table_name + " GROUP BY " + date_column + " ORDER BY " + date_column + " DESC LIMIT 1").collect()[0][date_column]
  if type(scoring_day) is datetime.date or type(scoring_day) is datetime:
    scoring_day = scoring_day.strftime("%Y-%m-%d")
  if is_qouted:
    scoring_day = "'" + scoring_day + "'"
  return scoring_day

# Select Dataframe from a table on specific date range.
# Return Spark dataframe
def selectRangeDataFrame(table_name,date_column,scoring_day,mode='daily',feature_range=90):
  if mode == 'daily':
    return spark.sql("SELECT * FROM " + table_name + " WHERE date(" + date_column + ") > date_sub(date(" + scoring_day + ")," + str(feature_range) + ") AND date(" + date_column + ") <= date(" + scoring_day + ")")
  elif mode == 'monthly':
    return spark.sql("SELECT * FROM " + table_name + " WHERE date(" + date_column + ") > add_months(date(" + scoring_day + ")," + str(feature_range*-1) + ") AND date(" + date_column + ") <= date(" + scoring_day + ")")

def buildAggDict(sdf,agg_func):
  agg_dict = {}
  for s in sdf.select([c for c in sdf.columns if c not in {'day_id', 'month_id', 'analytic_id', 'register_date', 'crm_subscription_id', 'ddate', 'register_status_date'}]).columns:
    agg_dict[s] = agg_func
  return agg_dict

# Pivot reccord-based Spark dataframe group by two keys on a specified column.
# Return Spark dataframe
def getPivotedDataFrame(sdf,key_1,key_2,pivot_column):
  agg_dict = buildAggDict(sdf,'first')
  return sdf.select([c for c in sdf.columns if c not in {'day_id','crm_subscription_id'}]).groupby(col(key_1), col(key_2)).pivot(pivot_column).agg(agg_dict)

def buildPivotedColumnNameList(sdf, key_1, key_2, feature_range):
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
  for i in range(feature_range):  
    for s in column_list:
      new_column_list.append(s+"_"+"tm"+str(feature_range-(i+1)))
  return column_list,new_column_list

def buildDailySourceFeature(table_name,date_column,pivot_column,key_1,key_2,scoring_day='0',mode='daily',feature_range=90):
  if scoring_day == '0':
    scoring_day = getMaxDate(table_name,date_column)
  print(scoring_day)
  sdf = selectRangeDataFrame(table_name, date_column, scoring_day, mode, feature_range)
  pivoted_sdf = getPivotedDataFrame(sdf,key_1,key_2,pivot_column)
  column_list,new_names = buildPivotedColumnNameList(pivoted_sdf,key_1,key_2,feature_range)
  pivoted_sdf = pivoted_sdf.toDF(*new_names)
  
  return pivoted_sdf, column_list, scoring_day

def buildDailySourceFeatureForMultipleMonth(table_name,date_column,pivot_column,key_1,key_2,scoring_day='0',mode='daily',feature_range=90, n=1):
  
  for i in range(n):
    if scoring_day == '0' or scoring_day is None:
      scoring_day = getMaxDate(table_name,date_column)
    month_id = (datetime.strptime(scoring_day, "'%Y-%m-%d'") - relativedelta(months=+i)).strftime("%Y-%m-%d")
    print(month_id)
    
  
    sdf = selectRangeDataFrame(table_name, date_column, "'{}'".format(month_id), mode, feature_range)
    pivoted_sdf = getPivotedDataFrame(sdf, key_1, key_2, pivot_column)
    column_list, new_names = buildPivotedColumnNameList(pivoted_sdf, key_1, key_2, feature_range)
    pivoted_sdf = pivoted_sdf.toDF(*new_names)
    
    pivoted_sdf = pivoted_sdf.select(pivoted_sdf.columns[:2] + [lit(month_id).alias("month_id")] +pivoted_sdf.columns[2:])
    
    if i == 0:
      pivoted_sdf_main = pivoted_sdf
    else:
      pivoted_sdf_main = pivoted_sdf_main.unionAll(pivoted_sdf)
    
  return pivoted_sdf_main, column_list, scoring_day

#Create List for generating periodic sum using sliding window method
#This function take spark dataframe, indexed list of the first numeric feature, original ordered numberic feature column list,
#stride size, window size, feature_range; number of data point, starting_period; for reuse/additional output naming convenient
#output features are created (according to sizing of datapoint-window_size)/stride_size + 1; the formula is created to handle full range of data pointxer_list,column_list)
###----------------------------------------- create parameter for velocity calculation --------------------------------------------####
# create sum of xy, x, x^, y, y^2
# create column for each num_iterate for each column_list

# generate sum of xy for velocity calculation
def slidingWindowedSumVelocityXYGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):
  
  x_list = [x for x in range(feature_range, 0, -1)] # create list of x for multiplying with y

  output_feature_list = []
  for i in range (0,len(column_list)):
    for j in range (0, num_iterate):
      
      output_feature_list.append(["sumv_xy_window_"+ column_list[i] + "_pm" + str(j+start_period),
                                  
                                  list( col(self.columns[ci+i]) for ci in indexer_list[(j*stride_size):window_size+(j*stride_size)] )
                                  + list (x_list[(j*2):window_size+(j*2)] )])    
  return output_feature_list

#Build aggregate sum feature from list
def buildSumVelocityXYFeature(self,output_feature_list, window_size):
  
  self = self.na.fill(0).select(['*'] + [sum(l[1][i] * l[1][i+window_size] for i in range(0, window_size)).alias(l[0]) for l in output_feature_list])  
  return self

# generate sum of y for velocity calculation
def slidingWindowedSumVelocityYGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):

  output_feature_list = []
  for i in range (0,len(column_list)):
    for j in range (0, num_iterate):
      
      output_feature_list.append(["sumv_y_window_"+ column_list[i] + "_pm" + str(j+start_period),                
                                  list( col(self.columns[ci+i]) for ci in indexer_list[(j*stride_size):window_size+(j*stride_size)] )])    
      
      
  return output_feature_list

#Build aggregate sum feature from list 
def buildSumVelocityYFeature(self,output_feature_list):

  self = self.na.fill(0).select(['*'] + [sum(k for k in l[1]).alias(l[0]) for l in output_feature_list])
 
  return self

# generate sum of y^2 for velocity calculation
def slidingWindowedSumVelocityYSquareGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):

  output_feature_list = []
  for i in range (0,len(column_list)):
    for j in range (0, num_iterate):
      
      output_feature_list.append(["sumv_y2_window_"+ column_list[i] + "_pm" + str(j+start_period),                  
                                  list( col(self.columns[ci+i]) for ci in indexer_list[(j*stride_size):window_size+(j*stride_size)] )])    
      
  return output_feature_list

#Build aggregate sum feature from list 
def buildSumVelocityYSquareFeature(self,output_feature_list):
  
  self = self.na.fill(0).select(['*'] + [sum(k**2 for k in l[1]).alias(l[0]) for l in output_feature_list])
  
  return self

# generate sum of x for velocity calculation
def slidingWindowedSumVelocityXGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):
  
  x_list = [i for i in range(feature_range, 0, -1)] # create list of x for summation
  
  output_feature_list = []
  for i in range (0,len(column_list)):
    for j in range (0, num_iterate):
      
      output_feature_list.append(["sumv_x_window_"+ column_list[i] + "_pm" + str(j+start_period), 
                                  x_list[(j*stride_size):window_size+(j*stride_size)] ])
      
  return output_feature_list

#Build aggregate sum feature from list 
def buildSumVelocityXFeature(self,output_feature_list):
  
  self = self.na.fill(0).select(['*'] + [lit(sum(k for k in l[1])).alias(l[0]) for l in output_feature_list])
  
  return self

# generate sum of x^2 for velocity calculation
def slidingWindowedSumVelocityXSquareGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):

  
  x_list = [i for i in range(feature_range, 0, -1)] # create list of x for summation
  
  output_feature_list = []
  for i in range (0,len(column_list)):
    for j in range (0, num_iterate):
      
      output_feature_list.append(["sumv_x2_window_"+ column_list[i] + "_pm" + str(j+start_period), 
                                  x_list[(j*stride_size):window_size+(j*stride_size)] ])

  return output_feature_list

#Build aggregate sum feature from list 
def buildSumVelocityXSquareFeature(self,output_feature_list):
  
  self = self.na.fill(0).select(['*'] + [lit(sum(k**2 for k in l[1])).alias(l[0]) for l in output_feature_list])
  
  return self

#############################################################################################################################
###------------------------------------------------ generate velocity ---------------------------------------------------####
# create m and b
# y2 -> l[1][0], y -> l[1][1], x2 -> l[1][2], x -> [1][3], xy -> l[1][4] for references
# create column for each num_iterate for each column

# generate m of velocity
def slidingWindowedVelocityMGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):
  
  output_feature_list = []
  k = 0
  for i in range (0,len(column_list)):
    for j in range (0, num_iterate):
      
      output_feature_list.append(["velo_m_window_"+ column_list[i] + "_wm" + str(j+start_period), list( col(self.columns[ci+k]) for ci in indexer_list )])
      k = k+1
  return output_feature_list

#Build aggregate sum feature from list 
def buildVelocityMFeature(self,output_feature_list, window_size):
  


  self = self.na.fill(0).select(['*'] + [( ((window_size*l[1][4]) - (l[1][3]*l[1][1]) ) / ( (window_size*l[1][2]) - (l[1][3]**2) )).alias(l[0]) for l in output_feature_list]) 
  return self

# generate b of velocity
def slidingWindowedVelocityBGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):

  output_feature_list = []
  k = 0
  for i in range (0,len(column_list)):
    for j in range (0, num_iterate):
      
      output_feature_list.append(["velo_b_window_"+ column_list[i] + "_wm" + str(j+start_period), list( col(self.columns[ci+k]) for ci in indexer_list )])         
      k = k+1
  return output_feature_list

def buildVelocityBFeature(self,output_feature_list, window_size):

  self = self.na.fill(0).select(['*'] + [( ((l[1][2]*l[1][1]) - (l[1][3]*l[1][4]) ) / ( (window_size*l[1][2]) - (l[1][3]**2) )).alias(l[0]) for l in output_feature_list]) 
  return self

#############################################################################################################################
###----------------------------------- create parameter for acceleration calculation ---------------------------------------####
# create sum of xy, x, x^, y, y^2
# create column for each column

# generate sum of xy for acceleration calculation 
def slidingWindowedSumAccelerationXYGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):

  x_list = [x for x in range(num_iterate, 0, -1)] # create list of x for multiplying with y

  output_feature_list = []
  for i in range (0, len(column_list)):
      
    output_feature_list.append(["suma_xy_window_"+ column_list[i],                                  
                                  list( col(self.columns[indexer_list[i]+ci]) for ci in range(0, num_iterate) ) + list (x_list)])      
    
  return output_feature_list

#Build aggregate sum feature from list 
def buildSumAccelerationXYFeature(self,output_feature_list, num_iterate):
  
  self = self.na.fill(0).select(['*'] + [sum(l[1][i] * l[1][i+num_iterate] for i in range(0, num_iterate)).alias(l[0]) for l in output_feature_list])  
  return self

# generate sum of y for acceleration calculation 
def slidingWindowedSumAccelerationYGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):
  
  output_feature_list = []
  for i in range (0, len(column_list)):
      
    output_feature_list.append(["suma_y_window_"+ column_list[i],                                  
                                  list( col(self.columns[indexer_list[i]+ci]) for ci in range(0, num_iterate) )])
    
  return output_feature_list

#Build aggregate sum feature from list 
def buildSumAccelerationYFeature(self,output_feature_list):

  self = self.na.fill(0).select(['*'] + [sum(k for k in l[1]).alias(l[0]) for l in output_feature_list])
  return self

# generate sum of y^2 for acceleration calculation 
def slidingWindowedSumAccelerationYSquareGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):

  output_feature_list = []
  for i in range (0, len(column_list)):
      
    output_feature_list.append(["suma_y2_window_"+ column_list[i],                                 
                                  list( col(self.columns[indexer_list[i]+ci]) for ci in range(0, num_iterate) )])
  return output_feature_list

#Build aggregate sum feature from list 
def buildSumAccelerationYSquareFeature(self,output_feature_list):
  
  self = self.na.fill(0).select(['*'] + [sum(k**2 for k in l[1]).alias(l[0]) for l in output_feature_list])
  
  return self

# generate sum of x for acceleration calculation 
def slidingWindowedSumAccelerationXGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):
  
  x_list = [i for i in range(num_iterate, 0, -1)] # create list of x for summation
  output_feature_list = []
  for i in range (0, len(column_list)):
      
    output_feature_list.append(["suma_x_window_"+ column_list[i], x_list ])  
  return output_feature_list

#Build aggregate sum feature from list 
def buildSumAccelerationXFeature(self,output_feature_list):
  
  self = self.na.fill(0).select(['*'] + [lit(sum(k for k in l[1])).alias(l[0]) for l in output_feature_list])
  return self

# generate sum of x^2 for acceleration calculation 
def slidingWindowedSumAccelerationXSquareGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):

  x_list = [i for i in range(num_iterate, 0, -1)] # create list of x for summation
  output_feature_list = []
  for i in range (0, len(column_list)):
      
    output_feature_list.append(["suma_x2_window_"+ column_list[i], x_list ])  
  return output_feature_list

#Build aggregate sum feature from list 
def buildSumAccelerationXSquareFeature(self,output_feature_list):
  
  self = self.na.fill(0).select(['*'] + [lit(sum(k**2 for k in l[1])).alias(l[0]) for l in output_feature_list]) 
  return self

#############################################################################################################################
###------------------------------------------ calculate acceleration ----------------------------------------------####
# create m and b
# y2 -> l[1][0], y -> l[1][1], x2 -> l[1][2], x -> [1][3], xy -> l[1][4] for references
# create column for each column_list
  
# generate m of acceleration
def slidingWindowedAccelerationMGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):
  
  output_feature_list = []
  for i in range (0, len(column_list)):
    
    output_feature_list.append(["accel_m_window_"+ column_list[i], list( col(self.columns[ci+i]) for ci in indexer_list )])    
  return output_feature_list

#Build aggregate sum feature from list 
def buildAccelerationMFeature(self,output_feature_list, num_iterate):

  self = self.na.fill(0).select(['*'] + [( ((num_iterate*l[1][4]) - (l[1][3]*l[1][1]) ) / ( (num_iterate*l[1][2]) - (l[1][3]**2) )).alias(l[0]) for l in output_feature_list]) 
  return self

# generate b of acceleration
def slidingWindowedAccelerationBGenerator(self, indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate):

  output_feature_list = []
  for i in range (0, len(column_list)):
      
    output_feature_list.append(["accel_b_window_"+ column_list[i], list( col(self.columns[ci+i]) for ci in indexer_list )]) 
  return output_feature_list

def buildAccelerationBFeature(self,output_feature_list, num_iterate):
  
  self = self.na.fill(0).select(['*'] + [( ((l[1][2]*l[1][1]) - (l[1][3]*l[1][4]) ) / ( (num_iterate*l[1][2]) - (l[1][3]**2) )).alias(l[0]) for l in output_feature_list]) 
  return self

#############################################################################################################################
def buildVelocityandAcceleration():
  
  indexer_list = []
  if multiple_month_flag:
    output, column_list, scoring_day = buildDailySourceFeatureForMultipleMonth(table_name, date_column, pivot_column, key_1, key_2, scoring_day, mode, feature_range, num_month)
    for i in range(0, feature_range): #for every tm
      indexer_list.append(((len(column_list)*feature_range+3-1) - (len(column_list) - 1) - (i*len(column_list))))
  else:
    output, column_list, scoring_day = buildDailySourceFeature(table_name, date_column, pivot_column, key_1, key_2, scoring_day, mode, feature_range)
    for i in range(0, feature_range): #for every tm
      indexer_list.append(((len(column_list)*feature_range+2-1) - (len(column_list) - 1) - (i*len(column_list))))
  output.persist()

  # generate parameter of velocity calculation
  output_feature_list = slidingWindowedSumVelocityXYGenerator(output,indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumVelocityXYFeature(output, output_feature_list, window_size)

  output_feature_list = slidingWindowedSumVelocityXGenerator(output,indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumVelocityXFeature(output, output_feature_list)

  output_feature_list = slidingWindowedSumVelocityXSquareGenerator(output,indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumVelocityXSquareFeature(output, output_feature_list)

  output_feature_list = slidingWindowedSumVelocityYGenerator(output,indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumVelocityYFeature(output, output_feature_list)

  output_feature_list = slidingWindowedSumVelocityYSquareGenerator(output,indexer_list, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumVelocityYSquareFeature(output, output_feature_list)

  #############################################################################################################################
  # calculate velocity
  starter = indexer_list[0] + len(column_list)
  indexer_list2 = []

  for i in range(0, 5): # 5 for x, x2, y, y2, xy
    indexer_list2.append(starter + (i*len(column_list)*num_iterate))
  indexer_list2.reverse()

  output_feature_list = slidingWindowedVelocityMGenerator(output,indexer_list2, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildVelocityMFeature(output, output_feature_list, window_size)

  output_feature_list = slidingWindowedVelocityBGenerator(output,indexer_list2, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildVelocityBFeature(output, output_feature_list, window_size)


  #############################################################################################################################
  # generate parameter of acceleration calculation

  starter = indexer_list2[0] + len(column_list)*num_iterate
  indexer_list3 = []

  for i in range(0, len(column_list)): # for every pm
    indexer_list3.append(starter + (i*num_iterate))
  output_feature_list = slidingWindowedSumAccelerationXYGenerator(output, indexer_list3, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumAccelerationXYFeature(output, output_feature_list, num_iterate)

  output_feature_list = slidingWindowedSumAccelerationXGenerator(output, indexer_list3, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumAccelerationXFeature(output, output_feature_list)

  output_feature_list = slidingWindowedSumAccelerationXSquareGenerator(output, indexer_list3, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumAccelerationXSquareFeature(output, output_feature_list)

  output_feature_list = slidingWindowedSumAccelerationYGenerator(output, indexer_list3, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumAccelerationYFeature(output, output_feature_list)

  output_feature_list = slidingWindowedSumAccelerationYSquareGenerator(output, indexer_list3, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildSumAccelerationYSquareFeature(output, output_feature_list)


  #############################################################################################################################
  # calculate acceleration
  starter = indexer_list3[len(indexer_list3)-1] + num_iterate + num_iterate*len(column_list)
  indexer_list4 = []

  for i in range(0, 5): # 5 for x, x2, y, y2, xy
    indexer_list4.append(starter + (i*len(column_list)))
  indexer_list4.reverse() # i do not know why i do this but it looks good

  output_feature_list = slidingWindowedAccelerationMGenerator(output,indexer_list4, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildAccelerationMFeature(output, output_feature_list, num_iterate)

  output_feature_list = slidingWindowedAccelerationBGenerator(output,indexer_list4, column_list, stride_size, window_size, feature_range, start_period, num_iterate)
  output = buildAccelerationBFeature(output, output_feature_list, num_iterate)

  end_time = time.time()

  print("Using {} seconds".format(end_time - start_time))
  selected_columns = [s for s in output.columns if ("velo" in s or "accel" in s)]
  return output.select([key_1, key_2, "month_id"] + selected_columns).columns


df = buildVelocityandAcceleration()