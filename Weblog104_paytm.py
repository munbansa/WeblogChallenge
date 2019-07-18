
# coding: utf-8

# In[257]:

from pyspark.sql import SparkSession, Row, Column
from pyspark.sql.functions import (regexp_extract, count, countDistinct, sumDistinct, 
                                   lag, when, udf, sum, mean, avg, desc,min, max)
import datetime
import calendar
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DataType, TimestampType
from pyspark import SparkContext, SparkConf


from pyspark.sql.window import Window


# <b>creating session and reading log file</b>

# In[2]:

sp = SparkSession.builder.appName('weblog').getOrCreate()
sc = SparkContext.getOrCreate()
rddf = sc.textFile('2015_07_22_mktplace_shop_web_log_sample.log')



# <b>Function to extract required attributes such as timestamp hour minute second host and url.
# This function below is assuming the constant pattern</b>

# In[278]:

def CreatRows(line):
    words = line.split(" ")

    # Convert time to TimeStamp format
    datetime_time = datetime.datetime.strptime(words[0], '%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Convert time to timestamp format
    timestamp = calendar.timegm(datetime_time.utctimetuple())                 
    
    # Extract IP and port of client
    host, client_port = words[2].split(":")
    
    # Create Row object
    row = Row(
        #originalstamp = words[0], #complete timestamp
        timestamp = timestamp,           # Timestamp
              hour = datetime_time.hour,       # Hour from the timestamp
              minute = datetime_time.minute,   # Minute from the timestamp
              second = datetime_time.second,
              host = host, 
              url = words[12]) 
    
    return row


# <b>Function based on regex extract. Hence not assuming the pattern. This works on individual rows but not on full dataframe. Still to debug</b>

# In[280]:

def Reg_extract_att(line):
    try:
        host = re.search(host_pat, line).group(1)
    except:
        host = None
    #method = re.search(req_pat, line).group(1)
    try:
        request = re.search(req_pat, line).group(2)
    except:
        request = None
    #protocol = re.search(req_pat, line).group(3)
    try:
        stamp = re.search(time_pat, line).group(1)
    except:
        stamp = None
    #status = re.search(status_pat, line).group(1)
    
    # Convert time to TimeStamp format
    datetime_time = datetime.datetime.strptime(stamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Convert time to timestamp format
    timestamp = calendar.timegm(datetime_time.utctimetuple())
    # Create Row object
    row = Row(timestamp = timestamp,           # Timestamp
              hour = datetime_time.hour,       # Hour from the timestamp
              minute = datetime_time.minute,   # Minute from the timestamp
              second = datetime_time.second,
              host = host, 
              url = request,
            # status = status
             ) 
    
    return row


# In[225]:

type(calendar.timegm(datetime.datetime.strptime('2015-07-22T09:00:27.894580Z','%Y-%m-%dT%H:%M:%S.%fZ').utctimetuple()))


# In[281]:

schema = StructType([
        #StructField("originalstamp", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("hour", IntegerType(), True),
        StructField("minute", IntegerType(), True),
        StructField("second", IntegerType(), True),
        #StructField("status", StringType(), True),
        StructField("host", StringType(), True),
        StructField("url", StringType(), True)])


# In[282]:

row_log_data = rddf.map(lambda row: CreatRows(row))
log_df = sp.createDataFrame(row_log_data, schema)


# In[10]:

#log_df.na.drop(how = 'any')

log_df.where('timestamp is null').show()

log_df = log_df.na.drop(how = 'any')


# In[11]:

log_df.createOrReplaceTempView('people')


# In[18]:

temp_df = sp.sql('select *, lag(timestamp, 1, 0) over (partition by host order by timestamp) as prev_timestamp from people')

temp_df.createOrReplaceTempView('First_table')

#temp_df.filter(temp_df['prev_timestamp'] == 0).show()

#temp_df.show()


# In[19]:

Reconstruction_df = sp.sql('select *, ROW_NUMBER() over (partition by host order by timestamp) as Sessionum from(select * from First_table where (timestamp -prev_timestamp) >=600 or prev_timestamp = 0) sometable')


Reconstruction_df.createOrReplaceTempView('Rec_table')


# In[20]:

Reconstruction_df.show()


# In[226]:

Result_df = sp.sql('select ft.host, ft.url, ft.timestamp, ft.prev_timestamp, ft.hour, rt.Sessionum from First_table ft LEFT JOIN Rec_table rt on ft.host = rt.host and ft.timestamp = rt.timestamp and ft.url = rt.url order by ft.host, ft.timestamp')

Result_df.createOrReplaceTempView('Result')


# In[227]:

Result_df.show()


# <b>Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session</b>

# In[228]:


import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
Sessions_df = Result_df.withColumn("Sessionum_2",                                    func.last('Sessionum', True).over(Window.partitionBy('host').orderBy('timestamp').rowsBetween(-sys.maxsize, 0)))


# In[229]:

Sessions_df.show(50)


# In[230]:

newDf = Sessions_df.withColumn("new_prev", when(Sessions_df["prev_timestamp"] == 0,Sessions_df['timestamp']).                               when(Sessions_df["prev_timestamp"] != 0, Sessions_df['prev_timestamp']).otherwise(Sessions_df['prev_timestamp'] )) 
                                                                                                
#newDf.show()                                                                                          


# In[231]:

newDf = newDf.withColumn('gap', newDf['timestamp'] - newDf['new_prev'])


# <b>Determine total URL visits per session</b>

# In[232]:

#No. of total Url hits in each session by each host 
Total_url = newDf.groupBy('host', 'Sessionum_2').agg(count('url'))

Total_url.show()


# <b>Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session</b>

# In[66]:

unique_url = newDf.groupBy('host', 'Sessionum_2').agg(countDistinct('url'))


# <b>Determine the average session time</b>

# In[118]:

session_time = newDf.groupBy('host', 'Sessionum_2').agg(sum('gap'))

session_time.show()
#newDf.printSchema()
#Sessions_df.show()


# <b>Overall average session time</b>

# In[139]:

session_time.agg({'sum(gap)' : 'mean'}).show()


# <b>mean session time per user</b>

# In[147]:

avg_session_time = session_time.groupBy('host').mean('sum(gap)')
avg_session_time.columns
#avg_session_time.show()


# <b>Find the most engaged users, ie the IPs with the longest session times</b>

# In[152]:

avg_session_time.sort(desc('avg(sum(gap))')).show()


# In[154]:

avg_session_time.sort(desc('avg(sum(gap))').alias('Average session time'))


# <b>Predict the expected load (requests/second) in the next minute</b>

# In[155]:

#Lets make a table for load at each timestamp 
newDf.show()


# In[170]:

#min and max timestamp
newDf.select([min('timestamp').alias('start_time'), max('timestamp').alias('end_time')]).show()


# In[214]:

#total data spread in 18.5 hours
(1437599427-1437532806)/60


# In[238]:

#This is the load (request/second) table 
load_per_timestamp = newDf.groupBy('timestamp').agg(count('timestamp').alias('Load')).orderBy('timestamp')

load_per_timestamp.show()


# In[181]:

load_per_timestamp.count()


# In[184]:

load_per_timestamp.summary().show()


# In[189]:

load_per_timestamp = load_per_timestamp.withColumn('new_timestamp', (load_per_timestamp['timestamp'] - 1437532806))
load_per_timestamp.summary().show()


# In[267]:

load_per_timestamp.createOrReplaceTempView('loadtable')


# In[200]:

#To see that data is not equally spaced in time
sp.sql('select distinct(diffs) from (select new_timestamp, (new_timestamp - (lag(new_timestamp,1,0) over (partition by timestamp order by timestamp))) as diffs  from loadtable ) order by diffs').show()


# In[ ]:

#Ignore
hist = df.rdd  .map(lambda l: l['age'])  .histogram([1, 11, 21,31,41,51,61,71,81,91])
This will return a tuple with "age ranges" and their respective observation count, as:

([1, 11, 21, 31, 41, 51, 61, 71, 81, 91],
  [10, 10, 10, 10, 10, 10, 10, 10, 11])
Then you can convert that back to a data frame using:

#Use zip to link age_ranges to their counts
countTuples = zip(hist[0], hist[1])
#make a list from that
ageList = list(map(lambda l: Row(age_range=l[0], count=l[1]), countTuples))
sc.parallelize(ageList).toDF()


# In[217]:

#Ignore
hist = load_per_timestamp.rdd.map(lambda l:l['new_timestamp']).histogram(1110)

countTuples = zip(hist[0], hist[1])
timelist = list(map(lambda l: Row(time_range=l[0], count=l[1]), countTuples))
sc.parallelize(timelist).toDF().show()


# <b>Predict the session length for a given IP</b>

# In[223]:

newDf.groupBy('host', 'Sessionum_2').agg(sum('gap').alias('Session_length')).show()


# <b>Predicting session length for an IP as its mean ..It could be mode median as well. It can be cross validated by dividing data
# *Another way is to decode the ip address to map to an area and try to model accordingly. Not implementing here</b>

# In[224]:

Sl_df = newDf.groupBy('host', 'Sessionum_2').agg(sum('gap').alias('Session_length'))
Sl_df.groupBy('host').agg(avg('Session_length').alias('Average Session length per host')).show()


# <b>or we can predict hour wise average but it wont make as a session can last for more than an hour </b>

# In[233]:

(newDf.groupBy('host', 'hour', 'Sessionum_2').agg(sum('gap').alias('Session_length_vs_hour'))).groupBy('host','hour').agg(avg('Session_length_vs_hour').alias('Average Session length per host vs hour'))


# <b>Predict the number of unique URL visits by a given IP .. this can come from the mean of some hits or average of hour wise hit </b>

# In[234]:

newDf.groupBy('host').agg(countDistinct('url').alias('# of unique url hits')).show()


# <b>number of unique url hits by a given IP by taking average on hours. With this much data I am not sure modelling on hour wise makes sense so probably closest to hourly no. of hit can be the answer </b>

# In[236]:

newDf.groupBy('host', 'hour').agg(countDistinct('url').alias('# of unique url hits'))


# <b>Predict the expected load (requests/second) in the next minute</b>

# <b>timestamps are not evenly spaced to do any standard timeseries analysis. 
# This would either require interpolation of data or using a part of the data.
# Other algorithms such as linear regression are not making sense to me as it a time dependent stats</b>

# <b>Persistence model/ naive forecast</b>

# In[250]:

#Use lag function to predict the next timestamp load
w = Window.orderBy('timestamp')
load_per_timestamp.withColumn('pred_load', lag('Load',1, 0).over(w))


# In[276]:

#For next minute total load can be summed up last 60 consecutive timestamps

sp.sql('select sum(Load) from (select * from loadtable L order by timestamp desc LIMIT 60)').show()


# In[ ]:

#Other methods such as Moving average smoothing ARMA ARIMA can be used if data is equally spaced to start with. 


# In[ ]:



