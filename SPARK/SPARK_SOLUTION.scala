
val InputFile = "hdfs://localhost:9000/STOCK_MARKET_ASSIGNMENT/INPUT/STOCK_MARKET_DS.csv"
val OutputFile = "hdfs://localhost:9000/STOCK_MARKET_ASSIGNMENT/OUTPUT/SPARK/"
val SYMBOL = 0
val SERIES = 1
val OPEN = 2
val HIGH = 3
val LOW = 4
val CLOSE = 5
val LAST = 6
val PREVCLOSE = 7
val TOTTRDQTY = 8
val TOTTRDVAL = 9
val TIMESTAMP = 10
val TOTALTRADES = 11
val ISIN = 12
//hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT
//hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT/OUTPUT
//hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT/OUTPUT/SPARK/
//hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT/INPUT
//hdfs dfs -put /home/manav/Documents/STOCK_MARKET_ASSIGNMENT/DATA/STOCK_MARKET_DS.csv /STOCK_MARKET_ASSIGNMENT/INPUT/

//Q1 Use the given csv file as input data and implement following transformations:
//Filter Rows on specified criteria "Symbol equals GEOMETRIC"
//Select specific columns from those available: SYMBOL, OPEN, HIGH, LOW and CLOSE which meets above criteria
//Generate count of the number of rows from above result

//1.1
val STOCK = sc.textFile(InputFile).map(row => row.split(","))
val FilterMap = STOCK.filter(col => col(SYMBOL).toString == "GEOMETRIC").map(col => {(col(SYMBOL).toString, col(SERIES).toString, col(OPEN).toDouble, col(HIGH).toDouble, col(LOW).toDouble, col(CLOSE).toDouble, col(LAST).toDouble, col(PREVCLOSE).toDouble, col(TOTTRDQTY).toDouble, col(TOTTRDVAL).toDouble, col(TIMESTAMP).toString, col(TOTALTRADES).toLong, col(ISIN).toString)})
FilterMap.map(r => r.productIterator.mkString(",")).saveAsTextFile(OutputFile+"1_1")

//1.2
val colMap = STOCK.filter(col => col(SYMBOL).toString == "GEOMETRIC").map(col => {(col(SYMBOL).toString, col(OPEN).toDouble, col(HIGH).toDouble, col(LOW).toDouble, col(CLOSE).toDouble)})
val colMap = FilterMap.map(col => {(col._1, col._3, col._4, col._5, col._6)})
colMap.map(r => r.productIterator.mkString(",")).saveAsTextFile(OutputFile+"1_2")

//1.3
val countRDD = sc.parallelize(Seq(colMap.count(),""))
countRDD.saveAsTextFile(OutputFile+"1_3_1")

//Q2 Calculation of various statistical quantities and decision making:
//Only lines with value "EQ" in the "series" column should be processed. As the first stage, filter out all the lines that do not fulfil this criteria.
//For every stock, for every year, calculate the statistical parameters(min, max, mean and standard deviation)  and store the generated information in properly designated tables.

//2.1
val STOCK = sc.textFile(InputFile).map(row => row.split(","))
val FilterMap = STOCK.filter(col => col(SERIES).toString == "EQ").map(col => {(col(SYMBOL).toString, col(SERIES).toString, col(OPEN).toDouble, col(HIGH).toDouble, col(LOW).toDouble, col(CLOSE).toDouble, col(LAST).toDouble, col(PREVCLOSE).toDouble, col(TOTTRDQTY).toDouble, col(TOTTRDVAL).toDouble, col(TIMESTAMP).toString, col(TOTALTRADES).toLong, col(ISIN).toString)})
FilterMap.map(r => r.productIterator.mkString(",")).saveAsTextFile(OutputFile+"2_1")

//2.2
val stock_map = FilterMap.map(col => {(col._1.toString +"_"+ col._11.toString.split("-")(0),col._6)})
val stats_all = stock_map.groupByKey().mapValues(sq => (sq.min, sq.max, sq.sum/sq.size, org.apache.spark.util.StatCounter(sq).stdev))
val stats_map = stats_all.map(col => {(col._1.split("_")(0), col._2._1, col._2._2, BigDecimal(col._2._3).setScale(6,BigDecimal.RoundingMode.HALF_UP), BigDecimal(col._2._4).setScale(6, BigDecimal.RoundingMode.HALF_UP), col._1.split("_")(1))}).sortBy(_._6,false).sortBy(_._1)
stats_map.map(r => r.productIterator.mkString(",")).saveAsTextFile(OutputFile+"2_2")

//Q3 Select any year for which data is available:
//For the selected year, create a table that contains data only for those stocks that have an average total traded quntity of 3 lakhs or more per day. Print out the first 25 entries of the table and submit.
//From among these, select any 10 stocks from IT ("HCLTECH", "NIITTECH", "TATAELXSI","TCS", "INFY", "WIPRO", "DATAMATICS","TECHM","MINDTREE" and "OFSS") and create a table combining their data.
//Find out the Pearsons Correlation Coeffecient for every pair of stocks you have selected. Final output should be in decreasing order of the coeffecient. 

val STOCK = sc.textFile(InputFile).map(row => row.split(","))

//3.1
val FilterMap = STOCK.filter(col => col(TOTTRDQTY).toLong >= 300000 && col(TIMESTAMP).toString.split("-")(0) == "2017").map(col => {(col(SYMBOL).toString, col(SERIES).toString, col(OPEN).toDouble, col(HIGH).toDouble, col(LOW).toDouble, col(CLOSE).toDouble, col(LAST).toDouble, col(PREVCLOSE).toDouble, col(TOTTRDQTY).toDouble, col(TOTTRDVAL).toDouble, col(TIMESTAMP).toString, col(TOTALTRADES).toLong, col(ISIN).toString)})
val FilterMap25 = sc.parallelize(FilterMap.take(25))
FilterMap25.map(r => r.productIterator.mkString(",")).saveAsTextFile(OutputFile+"3_1")

//3.2
val ITSTOCK = FilterMap.filter(col => col._1 == "HCLTECH" || col._1 == "NIITTECH" || col._1 == "TATAELXSI" || col._1 == "TCS" || col._1 == "INFY" || col._1 == "WIPRO" || col._1 == "DATAMATICS" || col._1 == "TECHM" || col._1 == "MINDTREE" || col._1 == "OFSS")
ITSTOCK.map(r => r.productIterator.mkString(",")).saveAsTextFile(OutputFile+"3_2")

//3.3
val ITSTOCKA = ITSTOCK.map(col => {(col._11, col._1, col._6)}).map(col => (col._1, {(col._2, col._3)}))
val joined=ITSTOCKA.join(ITSTOCKA)
val fltr_join = joined.filter(col => col._2._1._1 > col._2._2._1).sortBy(_._2._1._1).sortBy(_._2._2._1).sortBy(_._1)
val fltr_map = fltr_join.map(col => col._2)
val fltr_map2 = fltr_map.map(col => ((col._1._1, col._2._1), (col._1._2, col._2._2, col._1._2*col._2._2)))
val grp = fltr_map2.groupByKey()
implicit def iterebleWithAvg(data:Iterable[Double]) = new {
    def avg:Double = data.sum / data.size
}
implicit def iterebleWithStdDev(data:Iterable[Double]) = new {
    def stddev:Double = org.apache.spark.util.StatCounter(data).stdev
}
val grp_map = grp.map(col => {((col._1._1, col._1._2), ((col._2.map(y=>y._3).avg - col._2.map(y=>y._1).avg * col._2.map(y=>y._2).avg)/(col._2.map(y=>y._1).stddev * col._2.map(y=>y._2).stddev)))})
val pearsoncoefficient = grp_map.map(col => (col._1._1, col._1._2, col._2)).sortBy(_._3, false)
pearsoncoefficient.map(r => r.productIterator.mkString(",")).saveAsTextFile(OutputFile+"3_3")
