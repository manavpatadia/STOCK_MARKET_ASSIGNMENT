import org.apache.spark.sql._
import spark.implicits._
import org.apache.spark
import org.apache.spark.sql.Row
val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()

case class STOCK_SCHEMA(SYMBOL:String, SERIES:String, OPEN:Double, HIGH:Double, LOW:Double, CLOSE:Double, LAST:Double, PREVCLOSE:Double, TOTTRDQTY:Double, TOTTRDVAL:Double, TIMESTAMP:String, TOTALTRADES:Int, ISIN:String)

val schemaSTOCK = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/DATA/STOCK_MARKET_DS.csv").as[STOCK_SCHEMA]
schemaSTOCK.createOrReplaceTempView("STOCK")


val a1_1 = spark.sql("SELECT * FROM STOCK WHERE SYMBOL = 'GEOMETRIC'")
a1_1.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/1_1")

val a1_2 = spark.sql("SELECT SYMBOL, OPEN, HIGH, LOW, CLOSE FROM STOCK WHERE SYMBOL = 'GEOMETRIC'")
a1_2.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/1_2")


val a1_3 = spark.sql("SELECT COUNT(*) AS NUM_ROWS FROM STOCK WHERE SYMBOL = 'GEOMETRIC'")
a1_3.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/1_3")

val a2_1 = spark.sql("SELECT * FROM STOCK WHERE SERIES = 'EQ'")
a2_1.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/2_1")

val a2_2 = spark.sql("SELECT SYMBOL, MIN(CLOSE) AS MIN_CLOSE, MAX(CLOSE) AS MAX_CLOSE, ROUND(AVG(CLOSE),6) AS AVG_CLOSE, ROUND(STDDEV_POP(CLOSE),6) AS STD_DEV_CLOSE, SUBSTR(TIMESTAMP,1,4) AS YEAR FROM STOCK WHERE SERIES = 'EQ' GROUP BY SYMBOL, SUBSTR(TIMESTAMP,1,4) ORDER BY SYMBOL, YEAR DESC")
a2_2.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/2_2")


val a3_1 = spark.sql("SELECT * FROM STOCK WHERE TOTTRDQTY >= 300000 AND SUBSTR(TIMESTAMP,1,4) = '2017'")
a3_1.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/3_1")
val schemaSTOCK2 = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/3_1/part-*.csv").as[STOCK_SCHEMA]
schemaSTOCK2.createOrReplaceTempView("STOCK_2017")

val a3_2 = spark.sql("SELECT * FROM STOCK_2017 WHERE SYMBOL IN ('HCLTECH', 'NIITTECH', 'TATAELXSI','TCS', 'INFY', 'WIPRO', 'DATAMATICS','TECHM','MINDTREE', 'OFSS')")
a3_2.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/3_2")
val schemaSTOCK3 = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/3_2/part-*.csv").as[STOCK_SCHEMA]
schemaSTOCK3.createOrReplaceTempView("ITSTOCK")

val a3_3 = spark.sql("SELECT SYMBOL1, SYMBOL2, ROUND((PSUM - (SUM1 * SUM2 / N)) / SQRT((SUM1SQ - POW(SUM1, 2.0) / N) * (SUM2SQ - POW(SUM2, 2.0) / N)),8) AS PEARSONCOEFFICIENT FROM (SELECT T1.SYMBOL AS SYMBOL1, T2.SYMBOL AS SYMBOL2, SUM(T1.CLOSE) AS SUM1, SUM(T2.CLOSE) AS SUM2, SUM(T1.CLOSE * T1.CLOSE) AS SUM1SQ, SUM(T2.CLOSE * T2.CLOSE) AS SUM2SQ, SUM(T1.CLOSE * T2.CLOSE) AS PSUM, COUNT(*) AS N FROM ITSTOCK AS T1 CROSS JOIN ITSTOCK AS T2 ON T1.TIMESTAMP=T2.TIMESTAMP WHERE T1.SYMBOL > T2.SYMBOL GROUP BY SYMBOL1, SYMBOL2 ORDER BY SYMBOL1 ASC,SYMBOL2 ASC) STEP1 ORDER BY PEARSONCOEFFICIENT DESC")
a3_3.repartition(1).repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/3_3")


//a1_1.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/1_1")
//a1_2.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/1_2")
//a1_3.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/1_3")
//a2_1.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/2_1")
//a2_2.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/2_2")
//a3_1.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/3_1")
//a3_2.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/3_2")
//a3_3.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/3_3")
//val a1_1 = spark.sql("SELECT * FROM STOCK WHERE SYMBOL = 'GEOMETRIC'")

