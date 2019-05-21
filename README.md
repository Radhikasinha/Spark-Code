# Spark-Code
All Spark and Map Reduce code is implemented on NYPD_Motor_Vehicle_WithOutHeader dataset
Below  pyspark code used for cleaning the data either in interactive mode or submit mode.

import sys

from pyspark import SparkContext, SparkConf

from pyspark.sql import SQLContext, SparkSession

from pyspark.sql.types import *

from pyspark.sql.functions import col

from operator import add


def dataprocessing(sc,filename):

        df =sc.read.csv(filename, header = True)
        
        df.show(2)
        
        new_df = df.select(df['#Date'], df['Borough'], df['ZIP Code'],df['NUMBER OF PERSONS INJURED'], df['NUMBER OF PERSONS KILLED'],df['NUMBER OF PEDESTRIANS KILLED'], df['NUMBER OF CYCLIST INJURED'],df['NUMBER OF CYCLIST KILLED'],df['NUMBER OF MOTORIST INJURED'], df['NUMBER OF MOTORIST KILLED'],df['VEHICLE TYPE CODE 1'])

        df1 = new_df.na.drop()
#cleaned data is saved as text file
        
        df1.repartition(1).write.format("csv").save("/user/sinhark/Data/cleanspark")

 
 if__name__== '__main__' :
        
        conf = SparkConf().setAppName("Spark CC Project2")
        sc = SparkSession.builder.master("local").appName("Spark cc Project2").getOrCreate()
        dataprocessing(sc,sys.argv[1])

Cleaned data is saved at location “/user/sinhark/Data/cleanspark/*txt file
Once the clean data is saved it can used  for processing data by running attached spark(spark_cc.py) file in submit mode and providing the cleaned data path as input in command.
Open nano spark_cc.py file in cluster
Copy the spark_cc.py(attached) file text provided and paste it in cluster console. Take care of Indentation while copying.
Attached spark code is used to Process task 2 and task3
It save the output as text file at location specified in the program.
Path is commented in spark_cc.py file.
Output of Task3 is attached with name part-00000. 


MapReduce code of Driver.Java and final output.java
Map Reduce Task
1.Use the same cleaned file saved during spark cleaning.
 2. Attached Driver.java file is executed for task 2.
Export the jar file and run it on cluster.
hadoop jar project2.jar Maincode.Driver /user/sinhark/Data/cleanspark2.txt /user/sinhark/New1/
Once executed out file will be generated at /user/sinhark/New1/part-r-00000
3.copy part-r-00000  file to local
hadoop fs -copyToLocal /user/sinhark/New1/part-r-00000 /home/sinhark/

3.Execute attached finalOutput.java by exporting jar file and running it on cluster.
hadoop jar project2.jar Maincode.finalOutput
Final output will be saved to out.txt file.
Attached is the file.
