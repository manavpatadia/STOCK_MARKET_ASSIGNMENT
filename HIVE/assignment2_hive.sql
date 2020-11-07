--2.1
SELECT * FROM NSEDATA WHERE SERIES != 'EQ';
--2.2
CREATE TABLE STOCKSTATS(SYMBOL STRING, MIN FLOAT, MAX FLOAT, MEAN FLOAT, STD FLOAT, YEAR STRING);

INSERT OVERWRITE TABLE STOCKSTATS 
SELECT SYMBOL, MIN(CLOSE), MAX(CLOSE), AVG(CLOSE), STDDEV_POP(CLOSE), 
FROM_UNIXTIME(UNIX_TIMESTAMP (MYDATE,'dd-MMM-YYYY'), 'YYYY') AS MD FROM NSEDATA GROUP BY SYMBOL, FROM_UNIXTIME(UNIX_TIMESTAMP (MYDATE,'dd-MMM-YYYY'), 'YYYY') ORDER BY SYMBOL, MD DESC;

mkdir STOCKSTATS;

INSERT OVERWRITE LOCAL DIRECTORY '/HOME/HDUSER/HIVE/SAVEFILES/STOCKSTATS' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

--3.1
--YEAR 2013
SELECT * FROM NSEDATA WHERE TOTTRDQTY >= 300000 AND
FROM_UNIXTIME(UNIX_TIMESTAMP (MYDATE,'DD-MMM-YYYY'), 'YYYY') = '2013' LIMIT 25; 


--3.2
--CREATING TABLE WITH ITSTOCK FOR YEAR 2013
CREATE TABLE ITSTOCK(SYMBOL STRING, SERIES STRING, OPEN FLOAT,
HIGH FLOAT, LOW FLOAT, CLOSE FLOAT, LAST FLOAT, PREVCLOSE FLOAT, TOTTRDATY INT, TOTTRDVAL FLOAT, MYDATE STRING, ISIN STRING, COLM STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

--INSERTING VALUES INTO ITSTOCK 58 INSERT OVERWRITE TABLE ITSTOCK 
SELECT * FROM NSEDATA WHERE SYMBOL IN
('HCLTECH', 'NIITTECH', 'TATAELXSI','TCS', 'INFY', 'WIPRO', 'DATAMATICS','TECHM','MINDTREE', 'OFSS') AND TOTTRDQTY > 300000
AND SERIES = 'EQ' AND FROM_UNIXTIME(UNIX_TIMESTAMP (MYDATE,'DD-MMM-YYYY'), 'YYYY') = '2013';

--CREATE TABLE ITSTOCKS SORTED WITH CROSS JOINS FOR CORR CALCULATION
CREATE TABLE ITSTOCKSORTED (SYMBOLI STRING, CLOSE1 FLOAT, SYMBOL2 STRING,
CLOSE2 FLOAT,MYDATE STRING);

INSERT INTO ITSTOCKSSORTED
INSERT OVERWRITE TABLE ITSTOCKSORTED SELECT T1. SYMBOL,T1.CLOSE, T2. SYMBOL, T2.CLOSE,
FROM_UNIXTIME(UNIX_TIMESTAMP (T1.MYDATE, 'DD-MMM-YYYY'), 'yyyy-MMM-dd')
AS MD FROM ITSTOCK T1 CROSS JOIN ITSTOCK T2 WHERE T2.SYMBOL>T1.SYMBOL AND
T1.MYDATE=T2.MYDATE AND TI.SERIES = 'EQ' ORDER BY T1.SYMBOL ASC,T2. SYMBOL
ASC, FROM_UNIXTIME(UNIX_TIMESTAMP(MD, 'yyyy-MMM-dd'), 'YYYY-MM-DD') ASC;

--CREATE TABLE TO STORE CORRELATION VALUES
CREATE TABLE PEARSONCORITSTOCK(SYMBOLI STRING, SYMBOL2 STRING, CORR FLOAT);

--INSERT CORRELATION VALUES INTO TABLE CORR
insert overwrite table pearsoncoritstock select symbol1, symbol2, (Avg close1*Close2) - (Avg(Close1) *Avg(Close2)))/(stddev_pop(Close1) * stddev_pop(Close2))
as PearsonCoefficient from itstocksorted group by symboli, symbol2 order by PearsonCoefficient desc;
hive -e "SELECT * FROM pearsoncoritstock" > /home/hduser/hive/savefiles/q2_3_3.csv
