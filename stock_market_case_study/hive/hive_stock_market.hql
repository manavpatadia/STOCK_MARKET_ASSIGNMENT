--hdfs dfs -mkdir /stock_market_case_study
--hdfs dfs -mkdir /stock_market_case_study/input
--hdfs dfs -put /home/manav/Desktop/stock_market_case_study/input/stock_market.csv /stock_market_case_study/input/

hive -e "create database stock_db"
--use stock_db

hive -e "create external table stock_db.stock_market_data (symbol string, series string, open double, high double, low double, close double, last double, prevclose double, tottrdqty int, tottrval double, mydate string, totaltrades int, isin string) row format delimited fields terminated by ',' stored as textfile location '/stock_market_case_study/input'"

--q1 use the given csv file as input data and implement following transformations:
--filter rows on specified criteria "symbol equals geometric"
--select specific columns from those available: symbol, open, high, low and close which meets above criteria
--generate count of the number of rows from above result

--1.1
hive -e "select * from  stock_db.stock_market_data where lower(symbol) = 'geometric'" > /home/manav/Desktop/stock_market_case_study/hive/output1_1.tsv

--1.2
hive -e "select symbol, open, high, low, close from stock_db.stock_market_data where lower(symbol) = 'geometric'" > /home/manav/Desktop/stock_market_case_study/hive/output1_2.tsv

--1.3
hive -e "select count(*) as num_rows from stock_db.stock_market_data where lower(symbol) = 'geometric'" > /home/manav/Desktop/stock_market_case_study/hive/output1_3.tsv

--q2 calculation of various statistical quantities and decision making:
--only lines with value "eq" in the "series" column should be processed. as the first stage, filter out all the lines that do not fulfil this criteria.
--for every stock, for every year, calculate the following statistical parameters and store the generated information in properly designated tables.

--2.1
hive -e "select * from  stock_db.stock_market_data where lower(series) = 'eq'" > /home/manav/Desktop/stock_market_case_study/hive/output2_1.tsv

--2.2
hive -e "select symbol, min(close), max(close), round(avg(close),6), round(stddev_pop(close),6), substr(mydate,1,4) as year from  stock_db.stock_market_data where lower(series) = 'eq' group by symbol, substr(mydate,1,4) order by symbol, year desc" > /home/manav/Desktop/stock_market_case_study/hive/output2_2.tsv

hive -e "create external table  stock_db.stock_statistical_param (symbol string, min float, max float, mean float, std float, year string) row format delimited fields terminated by ',' stored as textfile location '/home/manav/Desktop/stock_market_case_study/hive/output2_2.tsv'"


--q3 select any year for which data is available:
--for the selected year, create a table that contains data only for those stocks that have an average total traded quntity of 3 lakhs or more per day. print out the first 25 entries of the table and submit.
--from among these, select any 10 stocks from it ('hcltech', 'niittech', 'tataelxsi','tcs', 'infy', 'wipro', 'datamatics','techm','mindtree' and 'ofss') and create a table combining their data.
--find out the pearsons correlation coeffecient for every pair of stocks you have selected. final output should be in decreasing order of the coeffecient. 

--3.1
hive -e "create table stock_db.stock_market_data_2016(symbol string, series string, open double, high double, low double, close double, last double, prevclose double, tottrdqty int, tottrval double, mydate string, totaltrades int, isin string)"


hive -e "insert overwrite table stock_db.stock_market_data_2016 select * from  stock_db.stock_market_data where tottrdqty >= 300000 and substr(mydate,1,4) = '2016'"  

hive -e "select * from  stock_db.stock_market_data_2016 limit 25" > /home/manav/Desktop/stock_market_case_study/hive/output3_1.tsv

--3.2
--creating table with it_companies_stock for year 2016
hive -e "create table stock_db.it_companies_stock (symbol string, series string, open double, high double, low double, close double, last double, prevclose double, tottrdqty int, tottrval double, mydate string, totaltrades int, isin string)"

hive -e "insert overwrite table stock_db.it_companies_stock select * from  stock_db.stock_market_data_2016 where lower(symbol) in ('hcltech', 'niittech', 'tataelxsi','tcs', 'infy', 'wipro', 'datamatics','techm','mindtree', 'ofss')"

hive -e "select * from stock_db.it_companies_stock" > /home/manav/Desktop/stock_market_case_study/hive/output3_2.tsv

--3_3
hive -e "create table stock_db.it_companies_stock_close (symbol1 string, close1 float, symbol2 string, close2 float,mydate string)" 

hive -e "insert overwrite table stock_db.it_companies_stock_close select t1.symbol,t1.close, t2.symbol, t2.close, from_unixtime(unix_timestamp(t1.mydate, 'yyyy-mm-dd'), 'yyyy-mmm-dd') as md from stock_db.it_companies_stock t1 cross join stock_db.it_companies_stock t2 where t1.symbol > t2.symbol and t1.mydate=t2.mydate order by t1.symbol asc,t2.symbol asc, from_unixtime(unix_timestamp(md, 'yyyy-mm-dd'), 'yyyy-mmm-dd') asc"

hive -e "create table stock_db.it_pearsons_corr_coeff(symbol1 string, symbol2 string, corr float)"

hive -e "insert overwrite table stock_db.it_pearsons_corr_coeff select symbol1, symbol2, (avg(close1*close2) - (avg(close1) *avg(close2)))/(stddev_pop(close1) * stddev_pop(close2)) as pearsoncoefficient from stock_db.it_companies_stock_close group by symbol1, symbol2 order by pearsoncoefficient desc"

hive -e "select * from stock_db.it_pearsons_corr_coeff" > /home/manav/Desktop/stock_market_case_study/hive/output3_3.tsv



--hive -e "set hive.cli.print.header=true; select * from  stock_db.stock_market_data where lower(symbol) = 'geometric'" > /home/manav/Desktop/stock_market_case_study/hive/1_1.tsv
--hive -e "set hive.cli.print.header=true; select symbol, open, high, low, close from stock_db.stock_market_data where lower(symbol) = 'geometric'" > /home/manav/Desktop/stock_market_case_study/hive/1_2.tsv
--hive -e "set hive.cli.print.header=true; select count(*) as num_rows from stock_db.stock_market_data where lower(symbol) = 'geometric'" > /home/manav/Desktop/stock_market_case_study/hive/1_3.tsv
--hive -e "set hive.cli.print.header=true; select * from  stock_db.stock_market_data where lower(series) = 'eq'" > /home/manav/Desktop/stock_market_case_study/hive/2_1.tsv
--hive -e "set hive.cli.print.header=true; select symbol, min(close), max(close), round(avg(close),6), round(stddev_pop(close),6), substr(mydate,1,4) as year from  stock_db.stock_market_data where lower(series) = 'eq' group by symbol, substr(mydate,1,4) order by symbol, year desc" > /home/manav/Desktop/stock_market_case_study/hive/2_2.tsv
--hive -e "set hive.cli.print.header=true; select * from  stock_db.stock_market_data_2016 limit 25" > /home/manav/Desktop/stock_market_case_study/hive/3_1.tsv
--hive -e "set hive.cli.print.header=true; select * from stock_db.it_companies_stock" > /home/manav/Desktop/stock_market_case_study/hive/3_2.tsv
--hive -e "set hive.cli.print.header=true; select * from stock_db.it_pearsons_corr_coeff" > /home/manav/Desktop/stock_market_case_study/hive/3_3.tsv
