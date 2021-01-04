import org.apache.spark.sql._
import spark.implicits._
import org.apache.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()

case class STOCK_SCHEMA(SYMBOL:String, SERIES:String, OPEN:Double, HIGH:Double, LOW:Double, CLOSE:Double, LAST:Double, PREVCLOSE:Double, TOTTRDQTY:Double, TOTTRDVAL:Double, TIMESTAMP:String, TOTALTRADES:Int, ISIN:String)


//for importing data without headers

val STOCK_SCHEMA = StructType(List(StructField("SYMBOL", StringType,true), StructField("SERIES", StringType,true), StructField("OPEN", DoubleType,true), StructField("HIGH", DoubleType,true), StructField("LOW", DoubleType,true), StructField("CLOSE", DoubleType,true), StructField("LAST", DoubleType,true), StructField("PREVCLOSE", DoubleType,true), StructField("TOTTRDQTY", DoubleType,true), StructField("TOTTRDVAL", DoubleType,true), StructField("TIMESTAMP", StringType,true), StructField("TOTALTRADES", IntegerType,true), StructField("ISIN", StringType,true)))

val STOCK_DATA = sc.textFile("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/DATA/STOCK_MARKET_DS.csv").map(row => row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")).map(x => Row(x(0).toString, x(1).toString, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toString, x(11).toInt, x(12).toString))

val STOCK_DF = spark.createDataFrame(STOCK_DATA, STOCK_SCHEMA)


val a1_1 = STOCK_DF.filter("SYMBOL = 'GEOMETRIC'") 
a1_1.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/DF_1_1")

val a1_2 = a1_1.select("SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE")
a1_2.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/DF_1_2")

val a1_3 = spark.createDataFrame(sc.parallelize(Seq((" ", a1_2.count())))).toDF("_", "NumRows")
a1_3.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/DF_1_3")


val a2_1 = STOCK_DF.filter("SERIES = 'EQ'") 
a2_1.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/DF_2_1")


val a2_2 = a2_1.groupBy(col("SYMBOL"),substring(col("TIMESTAMP"),1,4).as("YEAR")).agg(min("CLOSE").as("Minimum"), max("CLOSE").as("Maximum"), avg("CLOSE").as("Average"), stddev("CLOSE").as("StdDev")).sort($"SYMBOL".desc, $"YEAR".desc)
a2_2.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/DF_2_2")

val a3_1 = STOCK_DF.filter("TOTTRDQTY >= 300000 AND SUBSTR(TIMESTAMP,1,4) = '2017'") 
a3_1.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/DF_3_1")

val a3_2 = a3_1.filter("SYMBOL IN ('HCLTECH', 'NIITTECH', 'TATAELXSI','TCS', 'INFY', 'WIPRO', 'DATAMATICS','TECHM','MINDTREE', 'OFSS')")
a3_2.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/DF_3_2")

case class ITSTOCK(SYMBOL1: String, SYMBOL2: String, CLOSE1: Double, CLOSE2: Double)

val ITSTOCKPairs = a3_2.as("T1").join(a3_2.as("T2"), $"T1.TIMESTAMP" === $"T2.TIMESTAMP" && $"T1.SYMBOL" > $"T2.SYMBOL").
select($"T1.SYMBOL".alias("SYMBOL1"),
       $"T2.SYMBOL".alias("SYMBOL2"),
       $"T1.CLOSE".alias("CLOSE1"),
       $"T2.CLOSE".alias("CLOSE2")
      ).repartition(100).as[ITSTOCK]

val a3_3 = ITSTOCKPairs.groupBy(col("SYMBOL1"), col("SYMBOL2")).agg(avg(col("CLOSE1")*col("CLOSE2")).as("AVG_C1C2"), avg(col("CLOSE1")).as("AVG_C1"), avg(col("CLOSE2")).as("AVG_C2"), stddev(col("CLOSE1")).as("StdDev_C1"), stddev(col("CLOSE2")).as("StdDev_C2")).select(col("SYMBOL1"), col("SYMBOL2"), ((col("AVG_C1C2")-(col("AVG_C1")*col("AVG_C1")))/(col("StdDev_C1")*col("StdDev_C2"))).as("PEARSONCOEFFICIENT")).sort($"PEARSONCOEFFICIENT".desc)
a3_3.repartition(1).write.format("csv").option("header", "true").save("/home/manav/Documents/STOCK_MARKET_ASSIGNMENT/SPARK/DF_3_3")


//SELECT SYMBOL1, SYMBOL2, (AVG(CLOSE1*CLOSE2) - (AVG(CLOSE1) *AVG(CLOSE2)))/(STDDEV_POP(CLOSE1) * STDDEV_POP(CLOSE2)) AS PEARSONCOEFFICIENT 
//FROM UC2_STOCK.ITSTOCKSORTED 
//GROUP BY SYMBOL1, SYMBOL2 
//ORDER BY PEARSONCOEFFICIENT DESC;


