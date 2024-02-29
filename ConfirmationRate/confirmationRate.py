# -*- coding: utf-8 -*-
"""
author sudhanshumbm@gmail.com
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,count,round
import pyspark.sql.functions as pysf
from pyspark.sql.types import IntegerType
spark=SparkSession.builder.appName('TR').getOrCreate()
cwd=os.getcwd()
dfSignups=spark.read.format('csv').options(header=True,inferSchema=True).load(cwd+'/input/signups.csv')
dfConfirmations=spark.read.format('csv').options(header=True,inferSchema=True).load(cwd+'/input/confirmations.csv')
dfTrans=dfConfirmations.withColumn('totConfirmations',when(dfConfirmations.action=='confirmed',1).otherwise(0))
dfTrans=dfTrans.groupBy('user_id').agg(count('action').alias('totRequested'),pysf.sum('totConfirmations').alias('totConfirmed'))
dfTrans=dfTrans.withColumn('confirmationRate',round(dfTrans.totConfirmed/dfTrans.totRequested,2))
dfSignups=dfSignups.withColumnRenamed('user_id','user_id_signup')
dfRes=dfSignups.join(dfTrans,(dfTrans.user_id==dfSignups.user_id_signup) ,'left')
dfRes=dfRes.na.fill({'confirmationRate':0})
dfRes=dfRes.select(dfRes.user_id_signup,dfRes.confirmationRate)
dfRes.show()
#dfRes.write.format('csv').options(header=True).save('C:/PySpark/res.csv')