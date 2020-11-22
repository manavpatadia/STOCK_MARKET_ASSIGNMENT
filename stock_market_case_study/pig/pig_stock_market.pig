
stock_market_data = load '/home/manav/Desktop/stock_market_case_study/input/stock_market.csv' using org.apache.pig.piggybank.storage.CSVLoader() as (symbol:chararray, series:chararray, open:double, high:double, low:double, close:double, last:double, prevclose:double, tottrdqty:double, tottrdval:double, timestamp:chararray, totaltrades:int, isin:chararray);

--q1 use the given csv file as input data and implement following transformations:
--filter rows on specified criteria "symbol equals geometric"
--select specific columns from those available: symbol, open, high, low and close which meets above criteria
--generate count of the number of rows from above result

--1.1
stock_fltr = filter stock_market_data by LOWER(symbol) == 'geometric';
store stock_fltr into '/home/manav/Desktop/stock_market_case_study/pig/output/1_1' using org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER') ;

--1.2
select_data = foreach stock_fltr generate symbol, open, high, low, close;
store select_data into '/home/manav/Desktop/stock_market_case_study/pig/output/1_2/' using org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

--1.3
grp_cols = group select_data all;
stock_count= foreach grp_cols generate COUNT(select_data.symbol) as (count_geo:long);
store stock_count into '/home/manav/Desktop/stock_market_case_study/pig/output/1_3/' using org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

--q2 calculation of various statistical quantities and decision making:
--only lines with value "eq" in the "series" column should be processed. as the first stage, filter out all the lines that do not fulfil this criteria.
--for every stock_market_data, for every year, calculate the statistical parameters(MIN, MAX, mean and standard deviation)  and store the generated information in properly designated tables.

--2.1
stock_market_data = load '/home/manav/Desktop/stock_market_case_study/input/stock_market.csv' using org.apache.pig.piggybank.storage.CSVLoader() as ( symbol:chararray, series:chararray, open:double, high:double, low:double, close:double, last:double, prevclose:double, tottrdqty:double, tottrdval:double, timestamp:chararray, totaltrades:int, isin:chararray);
stock_fltr = filter stock_market_data by LOWER(series) == 'eq';
store stock_fltr into '/home/manav/Desktop/stock_market_case_study/pig/output/2_1/' using org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

--2.2
grp = group stock_fltr by (symbol, SUBSTRING(timestamp,0,4));

statistics = foreach grp { 
    minimum = MIN(stock_fltr.close); maximum = MAX(stock_fltr.close); mean = AVG(stock_fltr.close); row_count = COUNT(stock_fltr.close); close_square = foreach stock_fltr generate close * close as (close_sq:double);
    generate group.$0 as (symbol:chararray), group.$1 as (year:int), minimum as (MIN:double), maximum as (MAX:double), ROUND_TO(mean,6) as (mean:double), row_count * 1.0 as (row_count:double), ROUND_TO(SUM(close_square.close_sq),4) as (sum_close_sq:double);
};
all_statistics = foreach statistics generate $0, $1, $2, $3, $4, ROUND_TO(SQRT(($6 / $5) - ($4 * $4)),6) as (stddev:double);
sort_statistics = order all_statistics by symbol, year desc;
store sort_statistics into '/home/manav/Desktop/stock_market_case_study/pig/output/2_2/' using org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');


--q3 select any year for which data is available:
--for the selected year, create a table that contains data only for those stocks that have an average total traded quntity of 3 lakhs or more per day. print out the first 25 entries of the table and submit.
--from among these, select any 10 stocks from it ('hcltech', 'niittech', 'tataelxsi','tcs', 'infy', 'wipro', 'datamatics','techm','mindtree' and 'ofss') and create a table combining their data.
--find out the pearsons correlation coeffecient for every pair of stocks you have selected. final output should be in decreasing order of the coeffecient. 

--3.1
stock_fltr = filter stock_market_data by tottrdqty >= 300000 and SUBSTRING(timestamp,0,4) == '2016';
limit_data = limit stock_fltr 25;
store limit_data into '/home/manav/Desktop/stock_market_case_study/pig/output/3_1/' using org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

--3.2
itstock = filter stock_fltr by LOWER(symbol) in ('hcltech', 'niittech', 'tataelxsi','tcs', 'infy', 'wipro', 'datamatics','techm','mindtree', 'ofss');
store itstock into '/home/manav/Desktop/stock_market_case_study/pig/output/3_2/' using org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

--3.3

stock_market_data = load '/home/manav/Desktop/stock_market_case_study/input/stock_market.csv' using org.apache.pig.piggybank.storage.CSVLoader() as ( symbol:chararray, series:chararray, open:double, high:double, low:double, close:double, last:double, prevclose:double, tottrdqty:double, tottrdval:double, timestamp:chararray, totaltrades:int, isin:chararray);
stock_fltr = filter stock_market_data by tottrdqty >= 300000 and SUBSTRING(timestamp,0,4) == '2016';
stock_it_1 = filter stock_fltr by LOWER(symbol) in ('hcltech', 'niittech', 'tataelxsi','tcs', 'infy', 'wipro', 'datamatics','techm','mindtree', 'ofss');
stock_it_2 = filter stock_fltr by LOWER(symbol) in ('hcltech', 'niittech', 'tataelxsi','tcs', 'infy', 'wipro', 'datamatics','techm','mindtree', 'ofss');
stock_it_1_2 = join stock_it_1 by timestamp, stock_it_2 by timestamp;
fltr = filter stock_it_1_2 by stock_it_1::symbol > stock_it_2::symbol;
group_symbol_1_2 = group fltr by (stock_it_1::symbol, stock_it_2::symbol);
correlation = foreach group_symbol_1_2 { 
    row_count = COUNT(fltr.stock_it_1::symbol);
    stats = foreach fltr generate stock_it_1::close as (close1:double), stock_it_2::close as (close2:double), stock_it_1::close * stock_it_1::close as (c1c1:double), stock_it_2::close * stock_it_2::close as (c2c2:double), stock_it_1::close * stock_it_2::close as (c1c2:double);
    generate group.$0 as (symbol1:chararray), group.$1 as (symbol2:chararray), SUM(stats.close1) as (sum_c1:double), SUM(stats.close2)  as (sum_c2:double), SUM(stats.c1c1) as (sum_c1c1:double), SUM(stats.c2c2) as (sum_c2c2:double), SUM(stats.c1c2) as (sum_c1c2:double), row_count * 1.0 as (num:double), fltr.$10 as timestamp;
};
correlation_stock_it = order correlation by symbol1, symbol2, timestamp;
pearsons_corr_coeff_it = foreach correlation generate symbol1, symbol2, ROUND_TO((sum_c1c2 - (sum_c1 * sum_c2 / num)) / SQRT((sum_c1c1 - (sum_c1 * sum_c1) / num) * (sum_c2c2 - (sum_c2 * sum_c2) / num)),8) as (pearsoncoefficient:double);
pearsons_corr_coeff_sort = order pearsons_corr_coeff_it by pearsoncoefficient desc;
store pearsons_corr_coeff_sort into '/home/manav/Desktop/stock_market_case_study/pig/output/3_3/' using org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

