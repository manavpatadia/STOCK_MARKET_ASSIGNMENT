InputFile = "/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/DATA/STOCK_MARKET_DS.csv"
OutputFile = "/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/OUTPUT/"
SYMBOL = 0
SERIES = 1
OPEN = 2
HIGH = 3
LOW = 4
CLOSE = 5
LAST = 6
PREVCLOSE = 7
TOTTRDQTY = 8
TOTTRD= 9
TIMESTAMP = 10
TOTALTRADES = 11
ISIN = 12
#hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT
#hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT/OUTPUT
#hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT/OUTPUT/SPARK/
#hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT/INPUT
#hdfs dfs -put /home/manav/Documents/STOCK_MARKET_ASSIGNMENT/DATA/STOCK_MARKET_DS.csv /STOCK_MARKET_ASSIGNMENT/INPUT/

#Q1 Use the given csv file as input data and implement following transformations:
#Filter Rows on specified criteria "Symbol equals GEOMETRIC"
#Select specific columns from those available: SYMBOL, OPEN, HIGH, LOW and CLOSE which meets above criteria
#Generate count of the number of rows from above result

#1.1
STOCK = sc.textFile(InputFile).filter(lambda x: x.split(",")[0] in ["GEOMETRIC"])
STOCK.repartition(1).saveAsTextFile(OutputFile+"py_1_1")

#1.2
colMap = STOCK.map(lambda x: x.split(",")).map(lambda x: (x[0], x[2], x[3], x[4], x[5]))
colMap.repartition(1).saveAsTextFile(OutputFile+"py_1_2")

#1.3
countRDD = sc.parallelize((colMap.count(),""))
countRDD.saveAsTextFile(OutputFile+"py_1_3")


#Q2 Calculation of various statistical quantities and decision making:
#Only lines with value "EQ" in the "series" column should be processed. As the first stage, filter out all the lines that do not fulfil this criteria.
#For every stock, for every year, calculate the statistical parameters(min, max, mean and standard deviation)  and store the generated information in properly designated tables.

#2.1
STOCK = sc.textFile(InputFile).filter(lambda x: x.split(",")[1] in ["EQ"])
STOCK.repartition(1).saveAsTextFile(OutputFile+"py_2_1")
#2.2
from pyspark.statcounter import StatCounter
stock_map = STOCK.map(lambda x: x.split(",")).map(lambda x: (str(x[0]), str(x[10]).split("-")[0], float(x[5]), float(x[5]), float(x[5]), float(x[5]))).toDF()
stats = stock_map.groupby('_1','_2').agg({'_3': 'min', '_4':'max', '_5': 'mean', '_6':'stddev'})
stats.rdd.map(list).repartition(1).saveAsTextFile(OutputFile+"py_2_3")


#Q3 Select any year for which data is available:
#For the selected year, create a table that contains data only for those stocks that have an average total traded quntity of 3 lakhs or more per day. Print out the first 25 entries of the table and submit.
#From among these, select any 10 stocks from IT ("HCLTECH", "NIITTECH", "TATAELXSI","TCS", "INFY", "WIPRO", "DATAMATICS","TECHM","MINDTREE" and "OFSS") and create a table combining their data.
#Find out the Pearsons Correlation Coeffecient for every pair of stocks you have selected. Final output should be in decreasing order of the coeffecient. 

#3.1
STOCK = sc.textFile(InputFile).map(lambda x: x.split(",")).filter(lambda x: float(x[8]) >= 300000 and x[10].split("-")[0] == "2017")
STOCK_25 = sc.parallelize(STOCK.take(25)).saveAsTextFile(OutputFile+"py_3_1")


#3.2
ITSTOCK = STOCK.filter(lambda x: str(x[0]) in ("HCLTECH", "NIITTECH", "TATAELXSI", "TCS", "INFY", "WIPRO", "DATAMATICS", "TECHM", "MINDTREE", "OFSS"))
ITSTOCK.repartition(1).saveAsTextFile(OutputFile+"py_3_2")

#3.3
ITSTOCKA = ITSTOCK.map(lambda x: (x[10], (x[0], x[5])))
joined=ITSTOCKA.join(ITSTOCKA)
fltr_join = joined.filter(lambda x: x[1][0][0] > x[1][1][0])
fltr_map = fltr_join.map(lambda x: x[1])
fltr_map2 = fltr_map.map(lambda x: (x[0][0], x[1][0], float(x[0][1]), float(x[1][1]), float(x[0][1]), float(x[1][1]), float(x[0][1]) * float(x[1][1]))) 
#symbol1, symbol2, close1, close2, close1, close2, close1*close2
#(avg(c1 * c2) - avg(c1) * avg(c2)) / (stddev(c1) * stddev(c2))
stats = fltr_map2.toDF().groupby('_1','_2').agg({'_3':'mean', '_4': 'mean', '_5': 'stddev_pop', '_6': 'stddev_pop', '_7': 'mean'})
+---------+----------+------------------+------------------+------------------+------------------+------------------+
|       _1|        _2|           avg(_3)|           avg(_7)|    stddev_pop(_6)|           avg(_4)|    stddev_pop(_5)|
+---------+----------+------------------+------------------+------------------+------------------+------------------+
+---------+----------+------------------+------------------+------------------+------------------+------------------+
|       0 |        1 |           2      |           3      |    4             |           5      |    6             |
+---------+----------+------------------+------------------+------------------+------------------+------------------+

stats_rdd = stats.rdd.map(list)
pearsoncoefficient = stats_rdd.map(lambda x: (((x[3] - x[2]*x[5]) / (x[4] * x[6]), (x[0], x[1])) if (x[4] != 0 and x[6] != 0) else (-99999,(x[0],x[1])))).sortByKey(False).map(lambda x: (x[1][0], x[1][1], x[0]))
pearsoncoefficient.repartition(1).saveAsTextFile(OutputFile+"py_3_34")





