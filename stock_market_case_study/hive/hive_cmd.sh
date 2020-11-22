hive -e "create table stock_db.stock_market_data_2016(symbol string, series string, open double, high double, low double, close double, last double, prevclose double, tottrdqty int, tottrval double, mydate string, totaltrades int, isin string)"

hive -e "insert overwrite table stock_db.stock_market_data_2016 select * from  stock_db.stock_market_data where tottrdqty >= 300000 and substr(mydate,1,4) = '2016'"  

hive -e "select * from  stock_db.stock_market_data_2016 limit 25" > /home/manav/Desktop/stock_market_case_study/hive/output3_1.tsv

#hive -e "create table stock_db.it_companies_stock (symbol string, series string, open double, high double, low double, close double, last double, prevclose double, tottrdqty int, tottrval double, mydate string, totaltrades int, isin string)"

hive -e "insert overwrite table stock_db.it_companies_stock select * from  stock_db.stock_market_data_2016 where lower(symbol) in ('hcltech', 'niittech', 'tataelxsi','tcs', 'infy', 'wipro', 'datamatics','techm','mindtree', 'ofss')"

hive -e "select * from stock_db.it_companies_stock" > /home/manav/Desktop/stock_market_case_study/hive/output3_2.tsv

#hive -e "create table stock_db.it_companies_stock_close (symbol1 string, close1 float, symbol2 string, close2 float,mydate string)" 

hive -e "insert overwrite table stock_db.it_companies_stock_close select t1.symbol,t1.close, t2.symbol, t2.close, from_unixtime(unix_timestamp(t1.mydate, 'yyyy-mm-dd'), 'yyyy-mmm-dd') as md from stock_db.it_companies_stock t1 cross join stock_db.it_companies_stock t2 where t1.symbol > t2.symbol and t1.mydate=t2.mydate order by t1.symbol asc,t2.symbol asc, from_unixtime(unix_timestamp(md, 'yyyy-mm-dd'), 'yyyy-mmm-dd') asc"

#hive -e "create table stock_db.it_pearsons_corr_coeff(symbol1 string, symbol2 string, corr float)"

hive -e "insert overwrite table stock_db.it_pearsons_corr_coeff select symbol1, symbol2, (avg(close1*close2) - (avg(close1) *avg(close2)))/(stddev_pop(close1) * stddev_pop(close2)) as pearsoncoefficient from stock_db.it_companies_stock_close group by symbol1, symbol2 order by pearsoncoefficient desc"

hive -e "select * from stock_db.it_pearsons_corr_coeff" > '/home/manav/Desktop/stock_market_case_study/hive/output3_3.tsv'
