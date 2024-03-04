# -*- coding: utf-8 -*-
"""
author sudhanshumbm@gmail.com
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,lag,lead
from pyspark.sql.window import Window

spark=SparkSession.builder.appName('SeatExchange').getOrCreate()
cwd=os.getcwd() 
df=spark.read.format('csv').options(header=True,inferSchema=True).load(cwd+'/input/seats.csv')
wf=Window.orderBy('id')
df=df.withColumn('leading',lead('student').over(wf))
df=df.withColumn('lagging',lag('student').over(wf))
df=df.withColumn('studentSwapped',when(df.id%2==0,df.lagging).otherwise(when(df.leading.isNull(),df.student).otherwise(df.leading)))
df=df['id',df.studentSwapped.alias('student')]
df.show()


