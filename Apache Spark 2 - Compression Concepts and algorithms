. Can compress to most of the logrithms(bzip2,deflate,uncompressed,lz4,gzip,snappy,none	)
. use option on spark.write before csv-
     df.write.option("codec","gzip").csv("<PATH>")
	 
	 
	 
order.write.csv("path to save")	 #without compress

#with compression

order.write.format('csv').option('codec','gzip').save("path to save")

#in case of json use compression insted of codec
order.write.option('compression','gzip').json("path to save with file_name")

#to compress parquet format files
spark.conf.set('spark.sql.parquet.compression.codec','gzip')

order.write.parquet('path-orders_parquet_compression')

# for avro files compression(not support gzip)

pyspark --master yarn --conf spark.ui.port=12901 --packages com.databricks:spark-avro_2.11:4.0.00  # first pass jar files coresspoding to avaro

spark.conf.set('spark.sql.avro.compression.codec','snappy')

order.write.format('com.databricks.spark.avro').save('/user/order_avro_compressed',mode='overwrite')

#for uncompresseing use below one

spark.conf.set('spark.sql.avro.compression.codec','uncompressed')

order.write.format('com.databricks.spark.avro').save('/user/order_avro_compressed',mode='overwrite')

#for read avro file
spark.read.format('com.databricks.spark.avro').load('/users/order_avro_files').show()

#for read parquet file
spark.read.format('parquet').load('/users/order_parquet_files').show()

#--------------------Criteris and Tips------------------------------------------------------------
hdfs fsck filename to get metadata of files

1. When we are reading compressed file data ,then it will read by one executor .

    review=sc.textFile('/user/review.csv.gz')
	review.count()  # one executor will run to process the data ,beacuse its a non splitable algorithm
