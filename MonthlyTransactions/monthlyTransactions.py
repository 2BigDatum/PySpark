# -*- coding: utf-8 -*-
"""
author sudhanshumbm@gmail.com
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField,IntegerType,StringType, DoubleType

spark=SparkSession.builder.appName('TR').getOrCreate()

cwd=os.getcwd()
rdd=spark.sparkContext.textFile(cwd+'/input/transactions.csv')

rows=rdd.map(lambda x: x.split(',')).collect()
schema=rows[3]

df=spark.createDataFrame(rows[4:],schema)
df=df.withColumn('id',df.id.cast(IntegerType()))
df=df.withColumn('amount',df.amount.cast(IntegerType()))
df=df.withColumn('trans_date',to_date(df.trans_date))
df=df.withColumn('y-m',concat(year(df.trans_date).cast(StringType()),month(df.trans_date).cast(StringType())))
df=df.withColumn('approvedInd',when(df.state=='approved',1).otherwise(0))
df=df.withColumn('approvedAmt',when(df.state=='approved',df.amount).otherwise(0))
df=df.groupBy(['country','y-m']).agg(count('state').alias('trans_count'),sum('approvedInd').alias('approvedCount'),sum('amount').alias('trans_tot_amt'),sum('approvedAmt').alias('approved_tot_amt'))

df.show()


