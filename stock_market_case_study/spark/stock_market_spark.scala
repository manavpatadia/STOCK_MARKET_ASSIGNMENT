//Q1 Use the given csv file as input data and implement fol4ing transformations:
//Filter Rows on specified criteria "0 equals GEOMETRIC"
//Select specific xumns from those available: 0, 2, 3, 4 and 5 which meets above criteria
//Generate count of the number of rows from above result

//1.1
val input = sc.textFile("/home/suparna/Desktop/stock_market_case_study/input/stock_market.csv").map(line => line.split(","))
val stock_filter = input.filter(x => x(0).toString == "GEOMETRIC")
val output_1_1 = stock_filter.map(x => {(x(0).toString, x(1).toString, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toString, x(11).toLong, x(12).toString)})
output_1_1.map(line => line.productIterator.mkString("\t")).repartition(1).saveAsTextFile("/home/suparna/Desktop/stock_market_case_study/spark/output_1_1")

//1.2
val stock_filter = input.filter(x => x(0).toString == "GEOMETRIC")
val output_1_2 = stock_filter.map(x => {(x(0).toString, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble)})
output_1_2.map(line => line.productIterator.mkString("\t")).repartition(1).saveAsTextFile("/home/suparna/Desktop/stock_market_case_study/spark/output_1_2")

//1.3
val output_1_3 = sc.parallelize(Seq(output_1_2.count(),""))
output_1_3.repartition(1).saveAsTextFile("/home/suparna/Desktop/stock_market_case_study/spark/output_1_3")

//Q2 Calculation of various statistical quantities and decision making:
//Only lines with value "EQ" in the "1" xumn should be processed. As the first stage, filter out all the lines that do not fulfil this criteria.
//For every input, for every year, calculate the statistical parameters(min, max, mean and standard deviation)  and store the generated information in properly designated tables.

//2.1
val input = sc.textFile("/home/suparna/Desktop/stock_market_case_study/input/stock_market.csv").map(line => line.split(","))
val stock_filter = input.filter(x => x(1).toString == "EQ")
val output_2_1 = stock_filter.map(x => {(x(0).toString, x(1).toString, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toString, x(11).toLong, x(12).toString)})
output_2_1.map(line => line.productIterator.mkString("\t")).repartition(1).saveAsTextFile("/home/suparna/Desktop/stock_market_case_study/spark/output_2_1")

//2.2
val input_map = output_2_1.map(x => {(x._1.toString +"_"+ x._11.toString.split("-")(0),x._6)})
val min_max_mean_std = input_map.groupByKey().mapValues(sq => (sq.min, sq.max, sq.sum/sq.size, org.apache.spark.util.StatCounter(sq).stdev))
val output_2_2 = min_max_mean_std.map(x => {(x._1.split("_")(0), x._2._1, x._2._2, BigDecimal(x._2._3).setScale(6,BigDecimal.RoundingMode.HALF_UP), BigDecimal(x._2._4).setScale(6, BigDecimal.RoundingMode.HALF_UP), x._1.split("_")(1))}).sortBy(_._6,false).sortBy(_._1)
output_2_2.map(line => line.productIterator.mkString("\t")).repartition(1).saveAsTextFile("/home/suparna/Desktop/stock_market_case_study/spark/output_2_2")

//Q3 Select any year for which data is available:
//For the selected year, create a table that contains data only for those inputs that have an average total traded quntity of 3 lakhs or more per day. Print out the first 25 entries of the table and submit.
//From among these, select any 10 inputs from IT ("HCLTECH", "NIITTECH", "TATAELXSI","TCS", "INFY", "WIPRO", "DATAMATICS","TECHM","MINDTREE" and "OFSS") and create a table combining their data.
//Find out the Pearsons Correlation Coeffecient for every pair of inputs you have selected. Final output should be in decreasing order of the coeffecient. 

val input = sc.textFile("/home/suparna/Desktop/stock_market_case_study/input/stock_market.csv").map(line => line.split(","))

//3.1
val stock_filter = input.filter(x => x(8).toLong >= 300000 && x(10).toString.split("-")(0) == "2016")
val stock_map = stock_filter.map(x => {(x(0).toString, x(1).toString, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toString, x(11).toLong, x(12).toString)})
val output_3_1 = sc.parallelize(stock_map.take(25))
output_3_1.map(line => line.productIterator.mkString("\t")).repartition(1).saveAsTextFile("/home/suparna/Desktop/stock_market_case_study/spark/output_3_1")

//3.2
val output_3_2 = stock_map.filter(x => x._1 == "HCLTECH" || x._1 == "NIITTECH" || x._1 == "TATAELXSI" || x._1 == "TCS" || x._1 == "INFY" || x._1 == "WIPRO" || x._1 == "DATAMATICS" || x._1 == "TECHM" || x._1 == "MINDTREE" || x._1 == "OFSS")
output_3_2.map(line => line.productIterator.mkString("\t")).repartition(1).saveAsTextFile("/home/suparna/Desktop/stock_market_case_study/spark/output_3_2")

//3.3

implicit def calc_avg(x:Iterable[Double]) = new {
    def average:Double = x.sum / x.size
}
implicit def calc_std_dev(x:Iterable[Double]) = new {
    def standard_deviation:Double = org.apache.spark.util.StatCounter(x).stdev
}

val stock_IT1 = output_3_2.map(x => {(x._11, x._1, x._6)}).map(x => (x._1, {(x._2, x._3)}))
val stock_self_join=stock_IT1.join(stock_IT1)
val s1_ge_s2 = stock_self_join.filter(x => x._2._1._1 > x._2._2._1).sortBy(_._2._1._1).sortBy(_._2._2._1).sortBy(_._1)
val map1 = s1_ge_s2.map(x => x._2)
val map2 = map1.map(x => ((x._1._1, x._2._1), (x._1._2, x._2._2, x._1._2 * x._2._2)))
val group_symb1_symb2 = map2.groupByKey()
val pearsoncoefficient = group_symb1_symb2.map(x => {((x._1._1, x._1._2), ((x._2.map(y=>y._3).average - x._2.map(y=>y._1).average * x._2.map(y=>y._2).average)/(x._2.map(y=>y._1).standard_deviation * x._2.map(y=>y._2).standard_deviation)))})
val output_3_3 = pearsoncoefficient.map(x => (x._1._1, x._1._2, x._2.toDouble)).sortBy(_._3, false)
output_3_3.map(line => line.productIterator.mkString("\t")).repartition(1).saveAsTextFile("/home/suparna/Desktop/stock_market_case_study/spark/output_3_3")

