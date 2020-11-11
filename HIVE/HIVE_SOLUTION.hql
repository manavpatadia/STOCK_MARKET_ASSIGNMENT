--hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT
--hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT/OUTPUT
--hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/
--hdfs dfs -mkdir /STOCK_MARKET_ASSIGNMENT/INPUT
--hdfs dfs -put /home/manav/Documents/STOCK_MARKET_ASSIGNMENT/DATA/STOCK_MARKET_DS.csv /STOCK_MARKET_ASSIGNMENT/INPUT/
--SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN
--CREATE DATABASE UC2_STOCK;
USE UC2_STOCK;
CREATE EXTERNAL TABLE  UC2_STOCK.NSEDATA(SYMBOL STRING, SERIES STRING, OPEN DOUBLE, HIGH DOUBLE, LOW DOUBLE, CLOSE DOUBLE, LAST DOUBLE, PREVCLOSE DOUBLE, TOTTRDQTY INT, TOTTRVAL DOUBLE, MYDATE STRING, TOTALTRADES INT, ISIN STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/STOCK_MARKET_ASSIGNMENT/INPUT';

--Q1 Use the given csv file as input data and implement following transformations:
--Filter Rows on specified criteria "Symbol equals GEOMETRIC"
--Select specific columns from those available: SYMBOL, OPEN, HIGH, LOW and CLOSE which meets above criteria
--Generate count of the number of rows from above result

--1.1
INSERT OVERWRITE DIRECTORY '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/1_1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT * FROM  UC2_STOCK.NSEDATA WHERE SYMBOL = "GEOMETRIC";

--1.2
INSERT OVERWRITE DIRECTORY '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/1_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT SYMBOL, OPEN, HIGH, LOW, CLOSE FROM UC2_STOCK.NSEDATA WHERE SYMBOL = "GEOMETRIC";

--1.3
INSERT OVERWRITE DIRECTORY '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/1_3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT COUNT(*) AS NUM_ROWS FROM UC2_STOCK.NSEDATA WHERE SYMBOL = "GEOMETRIC";

--Q2 Calculation of various statistical quantities and decision making:
--Only lines with value "EQ" in the "series" column should be processed. As the first stage, filter out all the lines that do not fulfil this criteria.
--For every stock, for every year, calculate the following statistical parameters and store the generated information in properly designated tables.

--2.1
INSERT OVERWRITE DIRECTORY '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/2_1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT * FROM  UC2_STOCK.NSEDATA WHERE SERIES = 'EQ';
--2.2

INSERT OVERWRITE DIRECTORY '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/2_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT SYMBOL, MIN(CLOSE), MAX(CLOSE), ROUND(AVG(CLOSE),6), ROUND(STDDEV_POP(CLOSE),6), SUBSTR(MYDATE,1,4) AS YEAR 
FROM  UC2_STOCK.NSEDATA  
WHERE SERIES = 'EQ' 
GROUP BY SYMBOL, SUBSTR(MYDATE,1,4)
ORDER BY SYMBOL, YEAR DESC;

CREATE EXTERNAL TABLE  UC2_STOCK.STOCKSTATS
(SYMBOL STRING, MIN FLOAT, MAX FLOAT, MEAN FLOAT, STD FLOAT, YEAR STRING) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/2_2';


--Q3 Select any year for which data is available:
--For the selected year, create a table that contains data only for those stocks that have an average total traded quntity of 3 lakhs or more per day. Print out the first 25 entries of the table and submit.
--From among these, select any 10 stocks from IT ('HCLTECH', 'NIITTECH', 'TATAELXSI','TCS', 'INFY', 'WIPRO', 'DATAMATICS','TECHM','MINDTREE' and 'OFSS') and create a table combining their data.
--Find out the Pearsons Correlation Coeffecient for every pair of stocks you have selected. Final output should be in decreasing order of the coeffecient. 

--3.1

CREATE TABLE UC2_STOCK.STOCK_2017(SYMBOL STRING, SERIES STRING, OPEN DOUBLE, HIGH DOUBLE, LOW DOUBLE, CLOSE DOUBLE, LAST DOUBLE, PREVCLOSE DOUBLE, TOTTRDQTY INT, TOTTRVAL DOUBLE, MYDATE STRING, TOTALTRADES INT, ISIN STRING);

--select count(*) as records_per_year, SUBSTR(MYDATE,1,4) as year from UC2_STOCK.NSEDATA group by SUBSTR(MYDATE,1,4);

INSERT OVERWRITE TABLE UC2_STOCK.STOCK_2017
SELECT * FROM  UC2_STOCK.NSEDATA WHERE TOTTRDQTY >= 300000 AND SUBSTR(MYDATE,1,4) = '2017'; 

INSERT OVERWRITE DIRECTORY '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/3_1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT * FROM  UC2_STOCK.STOCK_2017 LIMIT 25;

--3.2
--CREATING TABLE WITH ITSTOCK FOR YEAR 2017
CREATE TABLE UC2_STOCK.ITSTOCK(SYMBOL STRING, SERIES STRING, OPEN DOUBLE, HIGH DOUBLE, LOW DOUBLE, CLOSE DOUBLE, LAST DOUBLE, PREVCLOSE DOUBLE, TOTTRDQTY INT, TOTTRVAL DOUBLE, MYDATE STRING, TOTALTRADES INT, ISIN STRING);

--INSERTING VALUES INTO UC2_STOCK.ITSTOCK
INSERT OVERWRITE TABLE UC2_STOCK.ITSTOCK 
SELECT * FROM  UC2_STOCK.STOCK_2017 
WHERE SYMBOL IN ('HCLTECH', 'NIITTECH', 'TATAELXSI','TCS', 'INFY', 'WIPRO', 'DATAMATICS','TECHM','MINDTREE', 'OFSS');

INSERT OVERWRITE DIRECTORY '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/3_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT * FROM  UC2_STOCK.ITSTOCK;

--3_3
--CREATE TABLE ITSTOCKS SORTED WITH CROSS JOINS FOR CORR CALCULATION
CREATE TABLE UC2_STOCK.ITSTOCKSORTED (SYMBOL1 STRING, CLOSE1 FLOAT, SYMBOL2 STRING, CLOSE2 FLOAT,MYDATE STRING);

INSERT OVERWRITE TABLE UC2_STOCK.ITSTOCKSORTED 
SELECT T1.SYMBOL,T1.CLOSE, T2.SYMBOL, T2.CLOSE, FROM_UNIXTIME(UNIX_TIMESTAMP(T1.MYDATE, 'yyyy-mm-dd'), 'yyyy-MMM-dd') AS MD 
FROM UC2_STOCK.ITSTOCK T1 
CROSS JOIN UC2_STOCK.ITSTOCK T2 
WHERE T1.SYMBOL > T2.SYMBOL AND T1.MYDATE=T2.MYDATE
ORDER BY T1.SYMBOL ASC,T2.SYMBOL ASC, FROM_UNIXTIME(UNIX_TIMESTAMP(MD, 'yyyy-mm-dd'), 'yyyy-MMM-dd') ASC;

--CREATE TABLE TO STORE CORRELATION VALUES
CREATE TABLE UC2_STOCK.PEARSONCORITSTOCK(SYMBOL1 STRING, SYMBOL2 STRING, CORR FLOAT);

--INSERT CORRELATION VALUES INTO TABLE CORR
INSERT OVERWRITE TABLE UC2_STOCK.PEARSONCORITSTOCK 
SELECT SYMBOL1, SYMBOL2, (AVG(CLOSE1*CLOSE2) - (AVG(CLOSE1) *AVG(CLOSE2)))/(STDDEV_POP(CLOSE1) * STDDEV_POP(CLOSE2)) AS PEARSONCOEFFICIENT 
FROM UC2_STOCK.ITSTOCKSORTED 
GROUP BY SYMBOL1, SYMBOL2 
ORDER BY PEARSONCOEFFICIENT DESC;

INSERT OVERWRITE DIRECTORY '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/3_3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT * FROM UC2_STOCK.PEARSONCORITSTOCK;

INSERT OVERWRITE TABLE UC2_STOCK.PEARSONCORITSTOCK 
SELECT SYMBOL1, SYMBOL2, ROUND((PSUM - (SUM1 * SUM2 / N)) / SQRT((SUM1SQ - POW(SUM1, 2.0) / N) * (SUM2SQ - POW(SUM2, 2.0) / N)),8) AS PEARSONCOEFFICIENT FROM
(SELECT T1.SYMBOL AS SYMBOL1, T2.SYMBOL AS SYMBOL2,
SUM(T1.CLOSE) AS SUM1, SUM(T2.CLOSE) AS SUM2, SUM(T1.CLOSE * T1.CLOSE) AS SUM1SQ, SUM(T2.CLOSE * T2.CLOSE) AS SUM2SQ, SUM(T1.CLOSE * T2.CLOSE) AS PSUM, COUNT(*) AS N  
FROM UC2_STOCK.ITSTOCK T1 
CROSS JOIN UC2_STOCK.ITSTOCK T2 
ON T1.MYDATE=T2.MYDATE 
WHERE T1.SYMBOL > T2.SYMBOL
GROUP BY T1.SYMBOL, T2.SYMBOL
ORDER BY T1.SYMBOL ASC,T2. SYMBOL ASC, FROM_UNIXTIME(UNIX_TIMESTAMP(T1.MYDATE, 'yyyy-mm-dd'), 'yyyy-MMM-dd') ASC) STEP1
ORDER BY PEARSONCOEFFICIENT DESC;

INSERT OVERWRITE DIRECTORY '/STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/3_3_7'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT * FROM UC2_STOCK.PEARSONCORITSTOCK;


hdfs dfs -cat /STOCK_MARKET_ASSIGNMENT/OUTPUT/HIVE/3_3/000000_0 | tail