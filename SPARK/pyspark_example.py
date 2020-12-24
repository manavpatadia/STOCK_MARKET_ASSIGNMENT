orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")

ordersFiltered = orders. \
filter(lambda o: o.split(",")[3] in ["COMPLETE", "CLOSED"])

ordersMap = ordersFiltered. \
map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderItemsMap = orderItems. \
map(lambda oi: 
     (int(oi.split(",")[1]), (int(oi.split(",")[2]), float(oi.split(",")[4])))
   )

ordersJoin = ordersMap.join(orderItemsMap)
ordersJoinMap = ordersJoin. \
map(lambda o: ((o[1][0], o[1][1][0]), o[1][1][1]))

from operator import add
dailyRevenuePerProductId = ordersJoinMap.reduceByKey(add)

productsRaw = open("/data/retail_db/products/part-00000"). \
read(). \
splitlines()
products = sc.parallelize(productsRaw)

productsMap = products. \
map(lambda p: (int(p.split(",")[0]), p.split(",")[2]))
dailyRevenuePerProductIdMap = dailyRevenuePerProductId. \
map(lambda r: (r[0][1], (r[0][0], r[1])))

dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productsMap)

dailyRevenuePerProduct = dailyRevenuePerProductJoin. \
map(lambda t: 
     ((t[1][0][0], -t[1][0][1]), (t[1][0][0], round(t[1][0][1], 2), t[1][1]))
   )
dailyRevenuePerProductSorted = dailyRevenuePerProduct.sortByKey()
dailyRevenuePerProductName = dailyRevenuePerProductSorted. \
map(lambda r: r[1])
dailyRevenuePerProductNameDF = dailyRevenuePerProductName. \
coalesce(2). \
toDF(schema=["order_date", "revenue_per_product", "product_name"])

dailyRevenuePerProductNameDF. \
save("/user/dgadiraju/daily_revenue_avro_python", "com.databricks.spark.avro")