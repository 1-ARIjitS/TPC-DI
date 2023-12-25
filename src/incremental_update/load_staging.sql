COPY staging.batchdate FROM '/home/workspace/data/sf_current/Batch2/BatchDate.txt';
COPY staging.cashtransaction_b2 FROM '/home/workspace/data/sf_current/Batch2/CashTransaction.txt' delimiter '|';
COPY staging.dailymarket_b2 FROM '/home/workspace/data/sf_current/Batch2/DailyMarket.txt' delimiter '|';
COPY staging.holdinghistory_b2 FROM '/home/workspace/data/sf_current/Batch2/HoldingHistory.txt' delimiter '|';
COPY staging.prospect FROM '/home/workspace/data/sf_current/Batch2/Prospect.csv' delimiter ',' CSV;
COPY staging.watchhistory_b2 FROM '/home/workspace/data/sf_current/Batch2/WatchHistory.txt' delimiter '|';
COPY staging.trade_b2 FROM '/home/workspace/data/sf_current/Batch2/Trade.txt' delimiter '|' null as '';
COPY staging.customer FROM '/home/workspace/data/sf_current/Batch2/Customer.txt' delimiter '|' null as '';
COPY staging.account FROM '/home/workspace/data/sf_current/Batch2/Account.txt' delimiter '|' null as '';
