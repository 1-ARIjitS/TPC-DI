INSERT INTO master.dimtrade(tradeid, sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, cashflag, quantity, bidprice, executedby, tradeprice, fee, commission, tax, status, type, sk_securityid, sk_companyid, sk_accountid, sk_customerid, sk_brokerid, batchid
)
SELECT
    t.t_id AS tradeid
-- If this is a new Trade record (CDC_FLAG = “I”) then SK_CreateDateID and SK_CreateTimeID must be set based on T_DTS. SK_CloseDateID and SK_CloseTimeID must be set to NULL.
  , to_char(t.t_dts::date, 'yyyymmdd')::numeric AS sk_createdateid
  , to_char(t.t_dts::time, 'hh24miss')::numeric AS sk_createtimeid
  , NULL AS sk_closedateid
  , NULL AS sk_closetimeid
-- TradeID, CashFlag, Quantity, BidPrice, ExecutedBy, TradePrice, Fee, Commission and Tax are copied from
-- T_ID, T_IS_CASH, T_QTY, T_BID_PRICE, T_EXEC_NAME, T_TRADE_PRICE, T_CHRG, T_COMM and T_TAX respectively.
  , t.t_is_cash = 1 AS cashflag
  , t.t_qty AS quantity
  , t.t_bid_price AS bidprice
  , t.t_exec_name AS executedby
  , t.t_trade_price AS tradeprice
  , t.t_chrg AS fee
  , t.t_comm AS commission
  , t.t_tax AS tax
-- Status is copied from ST_NAME of the StatusType table by matching T_ST_ID with ST_ID.
  , t.st_name AS status
-- Type is copied from TT_NAME of the TradeType table by matching T_TT_ID with TT_ID.
  , t.tt_name AS type
-- SK_SecurityID and SK_CompanyID are copied from SK_SecurityID and SK_CompanyID of the DimSecurity table by matching T_S_SYMB with Symbol where IsCurrent = 1. The match is guaranteed to succeed due to the referential integrity of the OLTP database. Note that these surrogate key values should reference the dimension record that is current at the earliest time this TradeID is encountered. If an update to a record is required in order to set the SK_CloseDateID and SK_CloseTimeID, these fields must not be updated. This dependency of DimTrade on DimSecurity requires that any update to a security’s DimSecurity records must be completed before updates to that security’s DimTrade records.
  , t.sk_securityid AS sk_securityid
  , t.sk_companyid AS sk_companyid
-- SK_AccountID, SK_CustomerID, and SK_BrokerID are copied from the SK_AccountID, SK_CustomerID, and SK_BrokerID fields of the DimAccount table by matching T_CA_ID with AccountID where IsCurrent = 1. The match is guaranteed to succeed due to the referential integrity of the OLTP database. Note that these surrogate key values must reference the dimension record that is current at the earliest time this TradeID is encountered. If an update to a record is required in order to set the SK_CloseDateID and SK_CloseTimeID, these fields must not be updated. This dependency of DimTrade on DimAccount requires that any update to an account’s DimAccount records must be completed before updates to that account’s DimTrade records.
  , t.sk_accountid AS sk_accountid
  , t.sk_customerid AS sk_customerid
  , t.sk_brokerid AS sk_brokerid
  , 2 AS batchid
FROM (
    SELECT *
    FROM (SELECT * FROM staging.trade_b2 WHERE cdc_flag = 'I') tb2
    INNER JOIN master.statustype st ON (tb2.t_st_id = st.st_id)  -- "StatusType is a static table: It is loaded from a file once in the Historical Load and not modified again."
    INNER JOIN master.tradetype tt ON (tb2.t_tt_id = tt.tt_id) -- "TradeType is a static table: It is loaded from a file once in the Historical Load and not modified again."
    INNER JOIN (SELECT * FROM master.dimsecurity WHERE iscurrent) ds ON (tb2.t_s_symb = ds.symbol) -- "No changes to DimSecurity will occur during Incremental Updates."
    INNER JOIN (SELECT * FROM master.DimAccount WHERE iscurrent) da ON (tb2.t_ca_id = da.accountid) -- DimAccount data is obtained from the data file Account.txt. CA_ID is the natural key for the Account data. The StatusType table will be referenced in the transformation.
    ORDER BY tb2.t_dts
) t
;

UPDATE master.dimtrade d
-- If T_ST_ID is “CMPT” or “CNCL”, SK_CloseDateID and SK_CloseTimeID must be set based on T_DTS.
SET sk_closedateid = case when t.t_st_id in ('CMPT', 'CNCL') then to_char(t.t_dts::date, 'yyyymmdd')::numeric end
  , sk_closetimeid = case when t.t_st_id in ('CMPT', 'CNCL') then to_char(t.t_dts::time, 'hh24miss')::numeric end
-- TradeID, CashFlag, Quantity, BidPrice, ExecutedBy, TradePrice, Fee, Commission and Tax are copied from
-- T_ID, T_IS_CASH, T_QTY, T_BID_PRICE, T_EXEC_NAME, T_TRADE_PRICE, T_CHRG, T_COMM and T_TAX respectively.
  , cashflag = t.t_is_cash = 1
  , quantity = t.t_qty
  , bidprice = t.t_bid_price
  , executedby = t.t_exec_name
  , tradeprice = t.t_trade_price
  , fee = t.t_chrg
  , commission = t.t_comm
  , tax = t.t_tax
-- Status is copied from ST_NAME of the StatusType table by matching T_ST_ID with ST_ID.
  , status = t.st_name
-- Type is copied from TT_NAME of the TradeType table by matching T_TT_ID with TT_ID.
  , type = t.tt_name
-- SK_SecurityID and SK_CompanyID are copied from SK_SecurityID and SK_CompanyID of the DimSecurity table by matching T_S_SYMB with Symbol where IsCurrent = 1. The match is guaranteed to succeed due to the referential integrity of the OLTP database. Note that these surrogate key values should reference the dimension record that is current at the earliest time this TradeID is encountered. If an update to a record is required in order to set the SK_CloseDateID and SK_CloseTimeID, these fields must not be updated. This dependency of DimTrade on DimSecurity requires that any update to a security’s DimSecurity records must be completed before updates to that security’s DimTrade records.
  , sk_securityid = t.sk_securityid
  , sk_companyid = t.sk_companyid
-- SK_AccountID, SK_CustomerID, and SK_BrokerID are copied from the SK_AccountID, SK_CustomerID, and SK_BrokerID fields of the DimAccount table by matching T_CA_ID with AccountID where IsCurrent = 1. The match is guaranteed to succeed due to the referential integrity of the OLTP database. Note that these surrogate key values must reference the dimension record that is current at the earliest time this TradeID is encountered. If an update to a record is required in order to set the SK_CloseDateID and SK_CloseTimeID, these fields must not be updated. This dependency of DimTrade on DimAccount requires that any update to an account’s DimAccount records must be completed before updates to that account’s DimTrade records.
  , sk_accountid = t.sk_accountid
  , sk_customerid = t.sk_customerid
  , sk_brokerid = t.sk_brokerid
  , batchid = 2
FROM (
    SELECT *
    FROM (SELECT * FROM staging.trade_b2 WHERE cdc_flag = 'U') tb2
    INNER JOIN master.statustype st ON (tb2.t_st_id = st.st_id)  -- "StatusType is a static table: It is loaded from a file once in the Historical Load and not modified again."
    INNER JOIN master.tradetype tt ON (tb2.t_tt_id = tt.tt_id) -- "TradeType is a static table: It is loaded from a file once in the Historical Load and not modified again."
    INNER JOIN (SELECT * FROM master.dimsecurity WHERE iscurrent) ds ON (tb2.t_s_symb = ds.symbol) -- "No changes to DimSecurity will occur during Incremental Updates."
    INNER JOIN (SELECT * FROM master.DimAccount WHERE iscurrent) da ON (tb2.t_ca_id = da.accountid) -- DimAccount data is obtained from the data file Account.txt. CA_ID is the natural key for the Account data. The StatusType table will be referenced in the transformation.
    ORDER BY tb2.t_dts
) t
WHERE tradeid = t.t_id
;
