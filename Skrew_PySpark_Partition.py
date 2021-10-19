Data Skew – Control Number of Records per Partition File

Use option maxRecordsPerFile if you want to control the number of records for each partition. 
This is particularly helpful when your data is skewed (Having some partitions with very low records and other partitions with high number of records)

#partitionBy() control number of partitions
df.write.option("header",True) \
        .option("maxRecordsPerFile", 2) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("/tmp/zipcodes-state")


Using repartition() and partitionBy() together

For each partition column, if you wanted to further divide into several partitions, use repartition() and partitionBy() together as explained in the below example.

repartition() creates specified number of partitions in memory. The partitionBy()  will write files to disk for each memory partition and partition column. 

Use repartition() and partitionBy() together
dfRepart.repartition(2)
        .write.option("header",True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("c:/tmp/zipcodes-state-more")
  
  
  
While you are create Data Lake out of Azure, HDFS or AWS you need to understand how to partition your data at rest (File system/disk), PySpark partitionBy() and repartition() help you partition the data and eliminating the Data Skew on your large datasets.

Hope this give you better idea on partitions in PySpark.
