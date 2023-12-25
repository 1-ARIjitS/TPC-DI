COPY staging.batchdate FROM '/home/workspace/data/sf_current/Batch3/BatchDate.txt';
COPY staging.cashtransaction_b2 FROM '/home/workspace/data/sf_current/Batch3/CashTransaction.txt' delimiter '|';
COPY staging.dailymarket_b2 FROM '/home/workspace/data/sf_current/Batch3/DailyMarket.txt' delimiter '|';
COPY staging.holdinghistory_b2 FROM '/home/workspace/data/sf_current/Batch3/HoldingHistory.txt' delimiter '|';
COPY staging.prospect FROM '/home/workspace/data/sf_current/Batch3/Prospect.csv' delimiter ',' CSV;
COPY staging.watchhistory_b2 FROM '/home/workspace/data/sf_current/Batch3/WatchHistory.txt' delimiter '|';
COPY staging.trade_b2 FROM '/home/workspace/data/sf_current/Batch3/Trade.txt' delimiter '|' null as '';
COPY staging.customer FROM '/home/workspace/data/sf_current/Batch3/Customer.txt' delimiter '|' null as '';
COPY staging.account FROM '/home/workspace/data/sf_current/Batch3/Account.txt' delimiter '|' null as '';
