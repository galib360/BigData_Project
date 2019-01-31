#!/usr/bin/env python
# coding: utf-8

# In[2]:


#########START
#spark.stop()
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
jan = spark.read.csv('../bigdata/yellow_tripdata_2017-01.csv', header=True, inferSchema = True)

loc = spark.read.csv('taxi _zone_lookup.csv', header=True, inferSchema = True)


# In[3]:


import pyspark.sql.functions

split_col = pyspark.sql.functions.split(jan['tpep_pickup_datetime'], ' ')
jan = jan.withColumn('PUDate', split_col.getItem(0))
jan = jan.withColumn('PUTime', split_col.getItem(1))
split_col = pyspark.sql.functions.split(jan['tpep_dropoff_datetime'], ' ')
jan = jan.withColumn('DODate', split_col.getItem(0))
jan = jan.withColumn('DOTime', split_col.getItem(1))

jan = jan.drop('tpep_pickup_datetime','tpep_dropoff_datetime')

df_joined = jan.join(loc,[jan.PULocationID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("LocationID as PULocID", "Borough as PUBorough", "Zone as PUZone", "passenger_count as passenger_count", "trip_distance as distance", "RateCodeID as RateCodeID", "PULocationID as srcID", "DOLocationID as dstID", "payment_type as payment_type", "fare_amount as fare", "extra as extra", "mta_tax as mta_tax", "tip_amount as tip", "tolls_amount as toll", "total_amount as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")
df_joined = df_joined.join(loc,[df_joined.dstID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("PULocID as PULocID", "PUBorough as PUBorough", "PUZone as PUZone", "LocationID as DOLocID", "Borough as DOBorough", "Zone as DOZone", "passenger_count as passenger_count", "distance as distance", "RateCodeID as RateCodeID", "srcID as srcID", "dstID as dstID", "payment_type as payment_type", "fare as fare", "extra as extra", "mta_tax as mta_tax", "tip as tip", "toll as toll", "total_fare as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")
df_joined.show()


# In[4]:


byTotalFare = df_joined.groupBy('PUZone', 'DOZone').avg('total_fare')
byDistance = df_joined.groupBy('PUZone', 'DOZone').max('distance')

from pyspark.sql.functions import lit
df_joined = df_joined.withColumn("month",lit(1))

Graph = df_joined.select('PUZone', 'DOZone', 'month')
Graph = Graph.groupBy('PUZone', 'DOZone').count()
Graph = Graph.withColumn("month",lit(1))
Graph = Graph.selectExpr("PUZone as src", "DOZone as dst", "count as count", "month as month")

byTotalFare = df_joined.groupBy('PUZone', 'DOZone').avg('total_fare')
Graph = Graph.join(byTotalFare,[Graph.src==byTotalFare.PUZone, Graph.dst==byTotalFare.DOZone],'left_outer')
Graph = Graph.drop('PUZone', 'DOZone')
Graph = Graph.withColumnRenamed("avg(total_fare)", "avg_fare")
Graph.show()


# In[5]:



########## max distance
max_dist = df_joined.orderBy('distance', ascending=False)
max_dist = max_dist.limit(10)
max_dist = max_dist.withColumnRenamed("distance", "max_distance")
max_dist = max_dist.select('PUZone', 'DOZone', 'max_distance', 'month')
max_dist.show()


# In[6]:



######max fare
max_fare = df_joined.orderBy('total_fare', ascending=False)
max_fare = max_fare.limit(10)
max_fare = max_fare.withColumnRenamed("total_fare", "max_fare")
max_fare = max_fare.select('PUZone', 'DOZone', 'max_fare', 'month')
max_fare.show()


# In[7]:


#### manipulate time
split_col = pyspark.sql.functions.split(df_joined['PUTime'], ':')
df_joined = df_joined.withColumn('PUHour', split_col.getItem(0))
df_joined = df_joined.withColumn('PUClock', split_col.getItem(0))

from pyspark.sql.functions import when

df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=0) & (df_joined["PUHour"] <6), 'midnight').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=6) & (df_joined["PUHour"] <12), 'morning').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=12) & (df_joined["PUHour"] <18), 'afternoon').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=18) & (df_joined["PUHour"] <=23), 'evening').otherwise(df_joined["PUHour"]))

df_joined.show(10)


# In[8]:


###### by time
PUByTime = df_joined.groupBy('PUHour', 'PUZone', 'month').count()
PUByTime = PUByTime.orderBy('count', ascending=False)
PUByTime.show(10)


# In[9]:


####### best PU locs
best_locs = df_joined.groupBy('PUZone', 'month').count()
best_locs = best_locs.orderBy('count', ascending=False)
best_locs.show(10)


# In[10]:


vertexP = df_joined.select("PUZone").distinct()
vertexP = vertexP.selectExpr("PUZone as Zone")
print(vertexP.count())
vertexD = df_joined.select("DOZone").distinct()
vertexD = vertexD.selectExpr("DOZone as Zone")
print(vertexD.count())
if vertexD.count() > vertexP.count():
  vertex = vertexD.unionAll(vertexP)
else : vertex = vertexP.unionAll(vertexD)
print(vertex.count())
vertex = vertex.select("Zone").distinct()
print(vertex.count())
vertex.toPandas().to_csv('Zones.csv', index=False)
vertex.show()


# In[11]:


edge = df_joined.groupBy('PUZone', 'DOZone').count()
edge = edge.selectExpr("PUZone as src", "DOZone as dst", "count as weight")
edge.toPandas().to_csv('Edges_jan.csv', index=False)
edge.show()


# In[12]:


#####################################################  
### Feb

jan = spark.read.csv('../bigdata/yellow_tripdata_2017-02.csv', header=True, inferSchema = True)
jan = jan.drop('VendorID','store_and_fwd_flag' ,'improvement_surcharge')

import pyspark.sql.functions

split_col = pyspark.sql.functions.split(jan['tpep_pickup_datetime'], ' ')
jan = jan.withColumn('PUDate', split_col.getItem(0))
jan = jan.withColumn('PUTime', split_col.getItem(1))
split_col = pyspark.sql.functions.split(jan['tpep_dropoff_datetime'], ' ')
jan = jan.withColumn('DODate', split_col.getItem(0))
jan = jan.withColumn('DOTime', split_col.getItem(1))

jan = jan.drop('tpep_pickup_datetime','tpep_dropoff_datetime')


# In[13]:


df_joined = jan.join(loc,[jan.PULocationID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("LocationID as PULocID", "Borough as PUBorough", "Zone as PUZone", "passenger_count as passenger_count", "trip_distance as distance", "RateCodeID as RateCodeID", "PULocationID as srcID", "DOLocationID as dstID", "payment_type as payment_type", "fare_amount as fare", "extra as extra", "mta_tax as mta_tax", "tip_amount as tip", "tolls_amount as toll", "total_amount as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")
df_joined = df_joined.join(loc,[df_joined.dstID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("PULocID as PULocID", "PUBorough as PUBorough", "PUZone as PUZone", "LocationID as DOLocID", "Borough as DOBorough", "Zone as DOZone", "passenger_count as passenger_count", "distance as distance", "RateCodeID as RateCodeID", "srcID as srcID", "dstID as dstID", "payment_type as payment_type", "fare as fare", "extra as extra", "mta_tax as mta_tax", "tip as tip", "toll as toll", "total_fare as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")
df_joined.show()


# In[14]:


byTotalFare = df_joined.groupBy('PUZone', 'DOZone').avg('total_fare')
byDistance = df_joined.groupBy('PUZone', 'DOZone').max('distance')

from pyspark.sql.functions import lit
df_joined = df_joined.withColumn("month",lit(2))

Grapht = df_joined.select('PUZone', 'DOZone', 'month')
Grapht = Grapht.groupBy('PUZone', 'DOZone').count()
Grapht = Grapht.withColumn("month",lit(2))
Grapht = Grapht.selectExpr("PUZone as src", "DOZone as dst", "count as count", "month as month")

byTotalFare = df_joined.groupBy('PUZone', 'DOZone').avg('total_fare')
Grapht = Grapht.join(byTotalFare,[Grapht.src==byTotalFare.PUZone, Grapht.dst==byTotalFare.DOZone],'left_outer')
Grapht = Grapht.drop('PUZone', 'DOZone')
Grapht = Grapht.withColumnRenamed("avg(total_fare)", "avg_fare")
Graph = Graph.union(Grapht)
Graph.show()


# In[15]:


########## max distance
max_distt = df_joined.orderBy('distance', ascending=False)
max_distt = max_distt.limit(10)
max_distt = max_distt.withColumnRenamed("distance", "max_distance")
max_distt = max_distt.select('PUZone', 'DOZone', 'max_distance', 'month')
max_dist = max_dist.union(max_distt)
max_dist.show()


# In[16]:


######max fare
max_faret = df_joined.orderBy('total_fare', ascending=False)
max_faret = max_faret.limit(10)
max_faret = max_faret.withColumnRenamed("total_fare", "max_fare")
max_faret = max_faret.select('PUZone', 'DOZone', 'max_fare', 'month')
max_fare = max_fare.union(max_faret)
max_fare.show()


# In[17]:


#### manipulate time
split_col = pyspark.sql.functions.split(df_joined['PUTime'], ':')
df_joined = df_joined.withColumn('PUHour', split_col.getItem(0))

from pyspark.sql.functions import when

df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=0) & (df_joined["PUHour"] <6), 'midnight').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=6) & (df_joined["PUHour"] <12), 'morning').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=12) & (df_joined["PUHour"] <18), 'afternoon').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=18) & (df_joined["PUHour"] <=23), 'evening').otherwise(df_joined["PUHour"]))

df_joined.show()


# In[18]:


###### by time
PUByTimet = df_joined.groupBy('PUHour', 'PUZone', 'month').count()
PUByTimet = PUByTimet.orderBy('count', ascending=False)
PUByTime = PUByTime.union(PUByTimet)
PUByTime.show()


# In[19]:


####### best PU locs
best_locst = df_joined.groupBy('PUZone', 'month').count()
best_locs = best_locs.union(best_locst)
best_locs.show()


# In[20]:


###############################################
#### March

jan = spark.read.csv('../bigdata/yellow_tripdata_2017-03.csv', header=True, inferSchema = True)
jan = jan.drop('VendorID','store_and_fwd_flag' ,'improvement_surcharge')

import pyspark.sql.functions

split_col = pyspark.sql.functions.split(jan['tpep_pickup_datetime'], ' ')
jan = jan.withColumn('PUDate', split_col.getItem(0))
jan = jan.withColumn('PUTime', split_col.getItem(1))
split_col = pyspark.sql.functions.split(jan['tpep_dropoff_datetime'], ' ')
jan = jan.withColumn('DODate', split_col.getItem(0))
jan = jan.withColumn('DOTime', split_col.getItem(1))

jan = jan.drop('tpep_pickup_datetime','tpep_dropoff_datetime')


# In[21]:


df_joined = jan.join(loc,[jan.PULocationID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("LocationID as PULocID", "Borough as PUBorough", "Zone as PUZone", "passenger_count as passenger_count", "trip_distance as distance", "RateCodeID as RateCodeID", "PULocationID as srcID", "DOLocationID as dstID", "payment_type as payment_type", "fare_amount as fare", "extra as extra", "mta_tax as mta_tax", "tip_amount as tip", "tolls_amount as toll", "total_amount as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")
df_joined = df_joined.join(loc,[df_joined.dstID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("PULocID as PULocID", "PUBorough as PUBorough", "PUZone as PUZone", "LocationID as DOLocID", "Borough as DOBorough", "Zone as DOZone", "passenger_count as passenger_count", "distance as distance", "RateCodeID as RateCodeID", "srcID as srcID", "dstID as dstID", "payment_type as payment_type", "fare as fare", "extra as extra", "mta_tax as mta_tax", "tip as tip", "toll as toll", "total_fare as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")
df_joined.show()


# In[22]:


byTotalFare = df_joined.groupBy('PUZone', 'DOZone').avg('total_fare')
byDistance = df_joined.groupBy('PUZone', 'DOZone').max('distance')

from pyspark.sql.functions import lit
df_joined = df_joined.withColumn("month",lit(3))

Grapht = df_joined.select('PUZone', 'DOZone', 'month')
Grapht = Grapht.groupBy('PUZone', 'DOZone').count()
Grapht = Grapht.withColumn("month",lit(3))
Grapht = Grapht.selectExpr("PUZone as src", "DOZone as dst", "count as count", "month as month")

byTotalFare = df_joined.groupBy('PUZone', 'DOZone').avg('total_fare')
Grapht = Grapht.join(byTotalFare,[Grapht.src==byTotalFare.PUZone, Grapht.dst==byTotalFare.DOZone],'left_outer')
Grapht = Grapht.drop('PUZone', 'DOZone')
Grapht = Grapht.withColumnRenamed("avg(total_fare)", "avg_fare")
Graph = Graph.union(Grapht)
Graph.show()


# In[23]:


########## max distance
max_distt = df_joined.orderBy('distance', ascending=False)
max_distt = max_distt.limit(10)
max_distt = max_distt.withColumnRenamed("distance", "max_distance")
max_distt = max_distt.select('PUZone', 'DOZone', 'max_distance', 'month')
max_dist = max_dist.union(max_distt)
max_dist.show()


# In[24]:


######max fare
max_faret = df_joined.orderBy('total_fare', ascending=False)
max_faret = max_faret.limit(10)
max_faret = max_faret.withColumnRenamed("total_fare", "max_fare")
max_faret = max_faret.select('PUZone', 'DOZone', 'max_fare', 'month')
max_fare = max_fare.union(max_faret)
max_fare.show()


# In[25]:


#### manipulate time
split_col = pyspark.sql.functions.split(df_joined['PUTime'], ':')
df_joined = df_joined.withColumn('PUHour', split_col.getItem(0))

from pyspark.sql.functions import when

df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=0) & (df_joined["PUHour"] <6), 'midnight').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=6) & (df_joined["PUHour"] <12), 'morning').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=12) & (df_joined["PUHour"] <18), 'afternoon').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=18) & (df_joined["PUHour"] <=23), 'evening').otherwise(df_joined["PUHour"]))
df_joined.show()


# In[26]:


###### by time
PUByTimet = df_joined.groupBy('PUHour', 'PUZone', 'month').count()
PUByTimet = PUByTimet.orderBy('count', ascending=False)
PUByTime = PUByTime.union(PUByTimet)
PUByTime.show()


# In[27]:


####### best PU locs
best_locst = df_joined.groupBy('PUZone', 'month').count()
best_locs = best_locs.union(best_locst)
best_locs.show()


# In[28]:


#################################################################
##### April
jan = spark.read.csv('../bigdata/yellow_tripdata_2017-04.csv', header=True, inferSchema = True)
jan = jan.drop('VendorID','store_and_fwd_flag' ,'improvement_surcharge')

import pyspark.sql.functions

split_col = pyspark.sql.functions.split(jan['tpep_pickup_datetime'], ' ')
jan = jan.withColumn('PUDate', split_col.getItem(0))
jan = jan.withColumn('PUTime', split_col.getItem(1))
split_col = pyspark.sql.functions.split(jan['tpep_dropoff_datetime'], ' ')
jan = jan.withColumn('DODate', split_col.getItem(0))
jan = jan.withColumn('DOTime', split_col.getItem(1))

jan = jan.drop('tpep_pickup_datetime','tpep_dropoff_datetime')


# In[29]:


df_joined = jan.join(loc,[jan.PULocationID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("LocationID as PULocID", "Borough as PUBorough", "Zone as PUZone", "passenger_count as passenger_count", "trip_distance as distance", "RateCodeID as RateCodeID", "PULocationID as srcID", "DOLocationID as dstID", "payment_type as payment_type", "fare_amount as fare", "extra as extra", "mta_tax as mta_tax", "tip_amount as tip", "tolls_amount as toll", "total_amount as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")
df_joined = df_joined.join(loc,[df_joined.dstID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("PULocID as PULocID", "PUBorough as PUBorough", "PUZone as PUZone", "LocationID as DOLocID", "Borough as DOBorough", "Zone as DOZone", "passenger_count as passenger_count", "distance as distance", "RateCodeID as RateCodeID", "srcID as srcID", "dstID as dstID", "payment_type as payment_type", "fare as fare", "extra as extra", "mta_tax as mta_tax", "tip as tip", "toll as toll", "total_fare as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")
df_joined.show()


# In[30]:


byTotalFare = df_joined.groupBy('PUZone', 'DOZone').avg('total_fare')
byDistance = df_joined.groupBy('PUZone', 'DOZone').max('distance')

from pyspark.sql.functions import lit
df_joined = df_joined.withColumn("month",lit(4))

Grapht = df_joined.select('PUZone', 'DOZone', 'month')
Grapht = Grapht.groupBy('PUZone', 'DOZone').count()
Grapht = Grapht.withColumn("month",lit(4))
Grapht = Grapht.selectExpr("PUZone as src", "DOZone as dst", "count as count", "month as month")

byTotalFare = df_joined.groupBy('PUZone', 'DOZone').avg('total_fare')
Grapht = Grapht.join(byTotalFare,[Grapht.src==byTotalFare.PUZone, Grapht.dst==byTotalFare.DOZone],'left_outer')
Grapht = Grapht.drop('PUZone', 'DOZone')
Grapht = Grapht.withColumnRenamed("avg(total_fare)", "avg_fare")
Graph = Graph.union(Grapht)
Graph.show()


# In[31]:


########## max distance
max_distt = df_joined.orderBy('distance', ascending=False)
max_distt = max_distt.limit(10)
max_distt = max_distt.withColumnRenamed("distance", "max_distance")
max_distt = max_distt.select('PUZone', 'DOZone', 'max_distance', 'month')
max_dist = max_dist.union(max_distt)
max_dist.show()


# In[32]:


######max fare
max_faret = df_joined.orderBy('total_fare', ascending=False)
max_faret = max_faret.limit(10)
max_faret = max_faret.withColumnRenamed("total_fare", "max_fare")
max_faret = max_faret.select('PUZone', 'DOZone', 'max_fare', 'month')
max_fare = max_fare.union(max_faret)
max_fare.show()


# In[33]:


#### manipulate time
split_col = pyspark.sql.functions.split(df_joined['PUTime'], ':')
df_joined = df_joined.withColumn('PUHour', split_col.getItem(0))

from pyspark.sql.functions import when

df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=0) & (df_joined["PUHour"] <6), 'midnight').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=6) & (df_joined["PUHour"] <12), 'morning').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=12) & (df_joined["PUHour"] <18), 'afternoon').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=18) & (df_joined["PUHour"] <=23), 'evening').otherwise(df_joined["PUHour"]))
df_joined.show()


# In[34]:


###### by time
PUByTimet = df_joined.groupBy('PUHour', 'PUZone', 'month').count()
PUByTimet = PUByTimet.orderBy('count', ascending=False)
PUByTime = PUByTime.union(PUByTimet)
PUByTime.show()


# In[35]:


####### best PU locs
best_locst = df_joined.groupBy('PUZone', 'month').count()
best_locs = best_locs.union(best_locst)
best_locs.show()


# In[36]:


import pandas as pd
Graph.toPandas().to_csv('Graph.csv', index=False)


# In[37]:


max_dist.toPandas().to_csv('Max_distance.csv', index=False)


# In[38]:


max_fare.toPandas().to_csv('Max_fare.csv', index=False)


# In[39]:


PUByTime.toPandas().to_csv('PickUpByTime.csv', index=False)


# In[40]:


best_locs.toPandas().to_csv('Best_PickUp_Locs.csv', index=False)


# In[41]:


spark.stop()


# In[1]:


#######################Cluster

import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
jan = spark.read.csv('../bigdata/yellow_tripdata_2017-01.csv', header=True, inferSchema = True)

loc = spark.read.csv('taxi _zone_lookup.csv', header=True, inferSchema = True)


# In[2]:


import pyspark.sql.functions

split_col = pyspark.sql.functions.split(jan['tpep_pickup_datetime'], ' ')
jan = jan.withColumn('PUDate', split_col.getItem(0))
jan = jan.withColumn('PUTime', split_col.getItem(1))
split_col = pyspark.sql.functions.split(jan['tpep_dropoff_datetime'], ' ')
jan = jan.withColumn('DODate', split_col.getItem(0))
jan = jan.withColumn('DOTime', split_col.getItem(1))

jan = jan.drop('tpep_pickup_datetime','tpep_dropoff_datetime')

df_joined = jan.join(loc,[jan.PULocationID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("LocationID as PULocID", "Borough as PUBorough", "Zone as PUZone", "passenger_count as passenger_count", "trip_distance as distance", "RateCodeID as RateCodeID", "PULocationID as srcID", "DOLocationID as dstID", "payment_type as payment_type", "fare_amount as fare", "extra as extra", "mta_tax as mta_tax", "tip_amount as tip", "tolls_amount as toll", "total_amount as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")
df_joined = df_joined.join(loc,[df_joined.dstID==loc.LocationID],'left_outer')
df_joined = df_joined.selectExpr("PULocID as PULocID", "PUBorough as PUBorough", "PUZone as PUZone", "LocationID as DOLocID", "Borough as DOBorough", "Zone as DOZone", "passenger_count as passenger_count", "distance as distance", "RateCodeID as RateCodeID", "srcID as srcID", "dstID as dstID", "payment_type as payment_type", "fare as fare", "extra as extra", "mta_tax as mta_tax", "tip as tip", "toll as toll", "total_fare as total_fare", "PUDate as PUDate", "PUTime as PUTime", "DODate as DODate", "DOTime as DOTime")

from pyspark.sql.functions import lit
df_joined = df_joined.withColumn("month",lit(1))

df_joined.show()


# In[3]:


#### manipulate time
split_col = pyspark.sql.functions.split(df_joined['PUTime'], ':')
df_joined = df_joined.withColumn('PUHour', split_col.getItem(0))
df_joined = df_joined.withColumn('PUClock', split_col.getItem(0))

from pyspark.sql.functions import when

df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=0) & (df_joined["PUHour"] <6), 'midnight').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=6) & (df_joined["PUHour"] <12), 'morning').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=12) & (df_joined["PUHour"] <18), 'afternoon').otherwise(df_joined["PUHour"]))
df_joined = df_joined.withColumn("PUHour", when((df_joined["PUHour"] >=18) & (df_joined["PUHour"] <=23), 'evening').otherwise(df_joined["PUHour"]))

df_joined.show(10)


# In[4]:


from pyspark.sql.types import IntegerType
df_joined = df_joined.withColumn("PUClock", df_joined["PUClock"].cast(IntegerType()))
df_joined.show(10)


# In[5]:


from pyspark.ml.feature import VectorAssembler
#vecAssembler = VectorAssembler(inputCols=["PULocID", "PUBorough", "PUZone", "DOLocID","DOBorough", "DOZone", "passenger_count" ,"distance" , "RateCodeID", "srcID", "dstID", "payment_type", "fare" ,"extra", "mta_tax",  "tip","toll","total_fare", "PUDate", "PUTime","DODate","DOTime","month" ,"PUHour"], outputCol="features")
vecAssembler = VectorAssembler(inputCols=["passenger_count" ,"distance" , "RateCodeID", "srcID", "dstID", "payment_type", "fare" ,"extra", "mta_tax","tip","toll","total_fare","month", "PUClock"], outputCol="features")
new_df = vecAssembler.transform(df_joined)
new_df.show()


# In[6]:


from pyspark.ml.clustering import KMeans

kmeans = KMeans(k=3, seed=1)  # 2 clusters here
model = kmeans.fit(new_df.select('features'))


# In[7]:


transformed = model.transform(new_df)
transformed.show()


# In[8]:


# load function
from pyspark.sql import functions as F
import pandas as pd

# aggregate data
stat = transformed.groupby('prediction').agg(
    F.mean(F.col('total_fare')).alias('avg_fare'),
    F.mean(F.col('distance')).alias('avg_distance'),
    F.mean(F.col('passenger_count')).alias('avg_passenger'),
    F.mean(F.col('tip')).alias('avg_tip'),
    F.variance(F.col('total_fare')).alias('var_fare'),
    F.variance(F.col('distance')).alias('var_distance'),
    F.variance(F.col('passenger_count')).alias('var_passenger'),
    F.variance(F.col('tip')).alias('var_tip')
)
stat.toPandas().to_csv('Cluster_stat_jan.csv', index=False)
stat.show()


# In[9]:


spark.stop()


# In[ ]:




