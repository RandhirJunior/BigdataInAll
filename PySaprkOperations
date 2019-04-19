ordeItems=sc.textFile('dbfs:/FileStore/tables/orderitems.txt')
orderitemsfilter=ordeItems.filter(lambda oi:int(oi.split(",")[1])==2)
orderitemsfilter.collect()
#output
#['2,2,1073,1,199.99,199.99', '3,2,502,5,250,50', '4,2,403,1,129.99,129.99']

orderitemap=orderitemsfilter.map(lambda oi:float(oi.split(",")[4]))
orderitemap.collect()
#[199.99, 250.0, 129.99]

orderitemap.reduce(lambda x,y:x+y)
#579.98

help(orderitemap)

# for Persisting data

from pyspark import StorageLevel
ordeItems.persist(StorageLevel.MEMORY_ONLY)
ordeItems.count()

#############################
list(range(1,10))

#[1, 2, 3, 4, 5, 6, 7, 8, 9]

----------------------------------
from operator import add
ordeItems=sc.textFile('dbfs:/FileStore/tables/orderitems.txt')
orderItemMap=ordeItems.map(lambda oi:(int(oi.split(",")[1]),float(oi.split(",")[4])))
orderItemSum=orderItemMap.reduceByKey(add)
for i in orderItemSum.take(10):print(i)

or 

ordeItems=sc.textFile('dbfs:/FileStore/tables/orderitems.txt')
orderItemMap=ordeItems.map(lambda oi:(int(oi.split(",")[1]),float(oi.split(",")[4])))
orderItemSum=orderItemMap.reduceByKey(lambda x,y:x+y)
for i in orderItemSum.take(10):print(i)
-------------------------------------




order_items= spark.read\
.format("jdbc")\
.option("url","jdbc:mysql://ms.itversity.com")\
.option("dbtable","retail_db.orders")\
.option("user","retail_user")\
.option("password","itversity").load()




