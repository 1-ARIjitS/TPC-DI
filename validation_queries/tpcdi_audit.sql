/* ++++++++++++++++++++++++++++++++++++++++++++++++++ *
 * +                                                + *
 * +        TPC-DI  Automated Audit Script          + *
 * +        Version 1.1.0                           + *
 * +                                                + *
 * ++++++++++++++++++++++++++++++++++++++++++++++++++ *
 *                                                    *
 *       ====== Portability Substitutions ======      *
 *     ---        [FROM DUMMY_TABLE]    ------        *
 * DB2            from sysibm.sysdummy1               *
 * ORACLE         from dual                           *
 * SQLSERVER       <blank>                            *
 * -------------------------------------------------- *
 *     ------  [||] (String concatenation) ------     *
 * SQLSERVER      +                                   *
 * -------------------------------------------------- *
 */

-- SELECT * FROM staging.audit LIMIT 50;
-- SELECT * FROM master.audit LIMIT 50;

-- The following commands have no impact on the stored data, but they speed up greatly the Audit queries.
CREATE INDEX IF NOT EXISTS idx_master_FactWatches_SK_DateID_DatePlaced ON master.FactWatches USING btree (SK_DateID_DatePlaced);
CREATE INDEX IF NOT EXISTS idx_master_DimDate_SK_DateID ON master.DimDate USING btree (SK_DateID);
CREATE INDEX IF NOT EXISTS idx_master_FactWatches_SK_DateID_DateRemoved ON master.FactWatches USING btree (SK_DateID_DateRemoved);
CREATE INDEX IF NOT EXISTS idx_master_DimTrade_SK_CreateDateID ON master.DimTrade USING btree (SK_CreateDateID);
CREATE INDEX IF NOT EXISTS idx_master_DimTrade_SK_CloseDateID ON master.DimTrade USING btree (SK_CloseDateID);
VACUUM ANALYZE;
SET random_page_cost = 1.1;

WITH audit_results AS (
select * from (

--
--  Checks against the Audit table.  If there is a problem with the Audit table, then other tests are suspect...
--

select
      'Audit table batches' as Test
     , NULL as Batch
     , case when
            (select count(distinct BatchID) from master.audit) = 3 and
            (select max(BatchID) from master.audit) = 3
        then 'OK'
        else 'Not 3 batches'
     end as Result
     , 'There must be audit data for 3 batches' as Description

union
select 'Audit table sources' as Test, NULL::numeric as Batch, case when
	(select count(distinct DataSet)
	 from master.audit
	 where DataSet in (
		'Batch',
		'DimAccount',
		'DimBroker',
		'DimCompany',
		'DimCustomer',
		'DimSecurity',
		'DimTrade',
--		'FactCashBalances', currently we are not generating any audit data for this table
		'FactHoldings',
		'FactMarketHistory',
		'FactWatches',
		'Financial',
		'Generator',
		'Prospect'
	) ) = 13
then 'OK' else 'Mismatch' end as Result, 'There must be audit data for all data sets' as Description

--
-- Checks against the DImessages table.
--

union
select 'DImessages validation reports', BatchID, Result, 'Every batch must have a full set of validation reports' from (
	select distinct BatchID, (
		case when
			(select count(*) from master.DImessages where BatchID = a.BatchID and MessageType = 'Validation') = 24
		then 'OK' else 'Validation checks not fully reported' end
	) as Result
	from master.DImessages a
) o

union
select 'DImessages batches', NULL, case when
	(select count(distinct BatchID) from master.DImessages) = 4 and
	(select max(BatchID) from master.DImessages) = 3
then 'OK' else 'Not 3 batches plus batch 0' end, 'Must have 3 distinct batches reported in DImessages'


union
select 'DImessages Phase complete records', NULL, case when
	(select count(distinct BatchID) from master.DImessages where MessageType = 'PCR') = 4 and
	(select max(BatchID) from master.DImessages where MessageType = 'PCR') = 3
then 'OK' else 'Not 4 Phase Complete Records' end, 'Must have 4 Phase Complete records'

union
select 'DImessages sources', NULL, case when
	(select count(*) from (
		select distinct MessageSource from master.DImessages where MessageType = 'Validation' and MessageSource in
			('DimAccount','DimBroker','DimCustomer','DimDate','DimSecurity','DimTime','DimTrade','FactCashBalances','FactHoldings',
			 'FactMarketHistory','FactWatches','Financial','Industry','Prospect','StatusType','TaxRate','TradeType')
	) a ) = 17
then 'OK' else 'Mismatch' end, 'Messages must be present for all tables/transforms'

union
select 'DImessages initial condition', NULL, case when
	(select count(*) from master.DImessages where BatchID = 0 and MessageType = 'Validation' and MessageData <> '0') = 0
then 'OK' else 'Non-empty table in before Batch1' end, 'All DW tables must be empty before Batch1'


--
-- Checks against the DimBroker table.
--
union
select 'DimBroker row count', NULL, case when
	(select count(*) from master.DimBroker) =
	(select Value from master.Audit where DataSet = 'DimBroker' and Attribute = 'HR_BROKERS')
then 'OK' else 'Mismatch' end, 'Actual row count matches Audit table'

union
select 'DimBroker distinct keys', NULL, case when
	(select count(distinct SK_BrokerID) from master.DimBroker) =
	(select Value from master.Audit where DataSet = 'DimBroker' and Attribute = 'HR_BROKERS')
then 'OK' else 'Not unique' end, 'All SKs are distinct'

union
select 'DimBroker BatchID', 1, case when
	(select count(*) from master.DimBroker where BatchID <> 1) = 0
then 'OK' else 'Not batch 1' end, 'All rows report BatchID = 1'

union
select 'DimBroker IsCurrent', NULL, case when
-- 	(select count(*) from master.DimBroker where IsCurrent <> 1) = 0
    (select count(*) from master.DimBroker where IsCurrent <> TRUE) = 0
then 'OK' else 'Not current' end, 'All rows have IsCurrent = 1'

union
select 'DimBroker EffectiveDate', NULL, case when
	(select count(*) from master.DimBroker where EffectiveDate <> '1950-01-01') = 0
then 'OK' else 'Wrong date' end, 'All rows have Batch1 BatchDate as EffectiveDate'

union
select 'DimBroker EndDate', NULL, case when
	(select count(*) from master.DimBroker where EndDate <> '9999-12-31') = 0
then 'OK' else 'Wrong date' end, 'All rows have end of time as EndDate'


--
-- Checks against the DimAccount table.
--
union
select 'DimAccount row count', 1, case when
	(select count(*) from master.DimAccount where BatchID = 1) >
	(select Value from master.Audit where DataSet = 'DimCustomer' and Attribute = 'C_NEW' and BatchID = 1) +
	(select Value from master.Audit where DataSet = 'DimAccount' and Attribute = 'CA_ADDACCT' and BatchID = 1) +
	(select Value from master.Audit where DataSet = 'DimAccount' and Attribute = 'CA_CLOSEACCT' and BatchID = 1) +
	(select Value from master.Audit where DataSet = 'DimAccount' and Attribute = 'CA_UPDACCT' and BatchID = 1) +
	(select Value from master.Audit where DataSet = 'DimCustomer' and Attribute = 'C_UPDCUST' and BatchID = 1) +
	(select Value from master.Audit where DataSet = 'DimCustomer' and Attribute = 'C_INACT' and BatchID = 1) -
	(select Value from master.Audit where DataSet = 'DimCustomer' and Attribute = 'C_ID_HIST' and BatchID = 1) -
	(select Value from master.Audit where DataSet = 'DimAccount' and Attribute = 'CA_ID_HIST' and BatchID = 1)
then 'OK' else 'Too few rows' end, 'Actual row count matches or exceeds Audit table minimum'

union
select 'DimAccount row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
	select distinct BatchID, (
		case when
			(select count(*) from master.DimAccount where BatchID = a.BatchID) >=
			(select Value from master.Audit where DataSet = 'DimAccount' and Attribute = 'CA_ADDACCT' and BatchID = a.BatchID) +
			(select Value from master.Audit where DataSet = 'DimAccount' and Attribute = 'CA_CLOSEACCT' and BatchID = a.BatchID) +
			(select Value from master.Audit where DataSet = 'DimAccount' and Attribute = 'CA_UPDACCT' and BatchID = a.BatchID) -
			(select Value from master.Audit where DataSet = 'DimAccount' and Attribute = 'CA_ID_HIST' and BatchID = a.BatchID)
		then 'OK' else 'Too few rows' end
	) as Result
	from master.Audit a
	where BatchID in (2, 3)
) o

union
select 'DimAccount distinct keys', NULL, case when
	(select count(distinct SK_AccountID) from master.DimAccount) =
	(select count(*) from master.DimAccount)
then 'OK' else 'Not unique' end, 'All SKs are distinct'


-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimAccount EndDate' checks that effective and end dates line up
--   'DimAccount Overlap' checks that there are not records that overlap in time
--   'DimAccount End of Time' checks that every company has a final record that goes to 9999-12-31

union
select 'DimAccount EndDate', NULL, case when
	(select count(*) from master.DimAccount) =
	(select count(*) from master.DimAccount a join master.DimAccount b on a.AccountID = b.AccountID and a.EndDate = b.EffectiveDate) +
	(select count(*) from master.DimAccount where EndDate = '9999-12-31')
then 'OK' else 'Dates not aligned' end, 'EndDate of one record matches EffectiveDate of another, or the end of time'

union
select 'DimAccount Overlap', NULL, case when (
	select count(*)
	from master.DimAccount a
	join master.DimAccount b on a.AccountID = b.AccountID and a.SK_AccountID <> b.SK_AccountID and a.EffectiveDate >= b.EffectiveDate and a.EffectiveDate < b.EndDate
) = 0
then 'OK' else 'Dates overlap' end, 'Date ranges do not overlap for a given Account'

union
select 'DimAccount End of Time', NULL, case when
	(select count(distinct AccountID) from master.DimAccount) =
	(select count(*) from master.DimAccount where EndDate = '9999-12-31')
then 'OK' else 'End of tome not reached' end, 'Every Account has one record with a date range reaching the end of time'

union
select 'DimAccount consolidation', NULL, case when
	(select count(*) from master.DimAccount where EffectiveDate = EndDate) = 0
then 'OK' else 'Not consolidated' end, 'No records become effective and end on the same day'

union
select 'DimAccount batches', NULL, case when
	(select count(distinct BatchID) from master.DimAccount) = 3  and
	(select max(BatchID) from master.DimAccount) = 3
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'

union
select 'DimAccount EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
	select distinct BatchID, (
		case when (
			select count(*) from master.DimAccount
			where BatchID = a.BatchID and (
				EffectiveDate < (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
				EffectiveDate > (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) )
		) = 0
		then 'OK' else 'Data out of range - see ticket #71' end
	) as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'DimAccount IsCurrent', NULL, case when
	(select count(*) from master.DimAccount) =
-- 	(select count(*) from master.DimAccount where EndDate = '9999-12-31' and IsCurrent = 1) +
-- 	(select count(*) from master.DimAccount where EndDate < '9999-12-31' and IsCurrent = 0)
    (select count(*) from master.DimAccount where EndDate = '9999-12-31' and IsCurrent = TRUE) +
	(select count(*) from master.DimAccount where EndDate < '9999-12-31' and IsCurrent = FALSE)
then 'OK' else 'Not current' end, 'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'

union
select 'DimAccount Status', NULL, case when
	(select count(*) from master.DimAccount where Status not in ('Active', 'Inactive')) = 0
then 'OK' else 'Bad value' end, 'All Status values are valid'

union
select 'DimAccount TaxStatus', NULL, case when
	(select count(*) from master.DimAccount where BatchID = 1 and TaxStatus not in (0, 1, 2)) = 0
then 'OK' else 'Bad value' end, 'All TaxStatus values are valid'

union
select 'DimAccount SK_CustomerID', NULL, case when
	(select count(*) from master.DimAccount) =
	(select count(*) from master.DimAccount a
	 join master.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID and c.EffectiveDate <= a.EffectiveDate and a.EndDate <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'

union
select 'DimAccount SK_BrokerID', NULL, case when
	(select count(*) from master.DimAccount) =
	(select count(*) from master.DimAccount a	 join master.DimBroker c on a.SK_BrokerID = c.SK_BrokerID and c.EffectiveDate <= a.EffectiveDate and a.EndDate <= c.EndDate)
then 'OK' else 'Bad join - spec problem with DimBroker EffectiveDate values' end, 'All SK_BrokerIDs match a broker record with a valid date range'

union
select 'DimAccount inactive customers', NULL, case when (
       select count(*) from
       (select count(*) from (select * from master.DimCustomer where Status='Inactive') c left join master.DimAccount a on a.SK_CustomerID = c.SK_CustomerID
          where a.Status = 'Inactive' group by c.SK_CustomerID having count(*) < 1) z
) = 0
then 'OK' else 'Bad value' end, 'If a customer is inactive, the corresponding accounts must also have been inactive'

--
-- Checks against the DimCustomer table.
--
union
select 'DimCustomer row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
	select distinct BatchID, case when
		(select count(*) from master.DimCustomer where BatchID = a.BatchID) >=
		(select Value from master.Audit where DataSet = 'DimCustomer' and Attribute = 'C_NEW' and BatchID = a.BatchID) +
		(select Value from master.Audit where DataSet = 'DimCustomer' and Attribute = 'C_INACT' and BatchID = a.BatchID) +
		(select Value from master.Audit where DataSet = 'DimCustomer' and Attribute = 'C_UPDCUST' and BatchID = a.BatchID) -
		(select Value from master.Audit where DataSet = 'DimCustomer' and Attribute = 'C_ID_HIST' and BatchID = a.BatchID)
	then 'OK' else 'Too few rows' end as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'DimCustomer distinct keys', NULL, case when
	(select count(distinct SK_CustomerID) from master.DimCustomer) =
	(select count(*) from master.DimCustomer)
then 'OK' else 'Not unique' end, 'All SKs are distinct'
 -- FROM DUMMY TABLE

-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimCustomer EndDate' checks that effective and end dates line up
--   'DimCustomer Overlap' checks that there are not records that overlap in time
--   'DimCustomer End of Time' checks that every company has a final record that goes to 9999-12-31

union
select 'DimCustomer EndDate', NULL, case when
	(select count(*) from master.DimCustomer) =
	(select count(*) from master.DimCustomer a join master.DimCustomer b on a.CustomerID = b.CustomerID and a.EndDate = b.EffectiveDate) +
	(select count(*) from master.DimCustomer where EndDate = '9999-12-31')
then 'OK' else 'Dates not aligned' end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
 -- FROM DUMMY TABLE

union
select 'DimCustomer Overlap', NULL, case when (
	select count(*)
	from master.DimCustomer a
	join master.DimCustomer b on a.CustomerID = b.CustomerID and a.SK_CustomerID <> b.SK_CustomerID and a.EffectiveDate >= b.EffectiveDate and a.EffectiveDate < b.EndDate
) = 0
then 'OK' else 'Dates overlap' end, 'Date ranges do not overlap for a given Customer'
 -- FROM DUMMY TABLE

union
select 'DimCustomer End of Time', NULL, case when
	(select count(distinct CustomerID) from master.DimCustomer) =
	(select count(*) from master.DimCustomer where EndDate = '9999-12-31')
then 'OK' else 'End of time not reached' end, 'Every Customer has one record with a date range reaching the end of time'
 -- FROM DUMMY TABLE

union
select 'DimCustomer consolidation', NULL, case when
	(select count(*) from master.DimCustomer where EffectiveDate = EndDate) = 0
then 'OK' else 'Not consolidated' end, 'No records become effective and end on the same day'
 -- FROM DUMMY TABLE

union
select 'DimCustomer batches', NULL, case when
	(select count(distinct BatchID) from master.DimCustomer) = 3 and
	(select max(BatchID) from master.DimCustomer) = 3
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
 -- FROM DUMMY TABLE

union
select 'DimCustomer IsCurrent', NULL, case when
	(select count(*) from master.DimCustomer) =
-- 	(select count(*) from master.DimCustomer where EndDate = '9999-12-31' and IsCurrent = 1) +
-- 	(select count(*) from master.DimCustomer where EndDate < '9999-12-31' and IsCurrent = 0)
    (select count(*) from master.DimCustomer where EndDate = '9999-12-31' and IsCurrent = TRUE) +
	(select count(*) from master.DimCustomer where EndDate < '9999-12-31' and IsCurrent = FALSE)
then 'OK' else 'Not current' end, 'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
 -- FROM DUMMY TABLE

union
select 'DimCustomer EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
	select distinct BatchID, (
		case when (
			select count(*) from master.DimCustomer
			where BatchID = a.BatchID and (
				EffectiveDate < (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
				EffectiveDate > (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) )
		) = 0
		then 'OK' else 'Data out of range' end
	) as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'DimCustomer Status', NULL, case when
	(select count(*) from master.DimCustomer where Status not in ('Active', 'Inactive')) = 0
then 'OK' else 'Bad value' end, 'All Status values are valid'
 -- FROM DUMMY TABLE



union
select 'DimCustomer inactive customers', BatchID, Result, 'Inactive customer count matches Audit table' from (
	select distinct BatchID, case when
		(select messageData from master.DImessages where MessageType = 'Validation' and BatchID = a.BatchID and 'DimCustomer' = MessageSource and 'Inactive customers' = MessageText) =
-- 		(select sum(Value) from master.Audit where DataSet = 'DimCustomer' and BatchID <= a.BatchID and Attribute = 'C_INACT')
	    (select sum(Value)::text from master.Audit where DataSet = 'DimCustomer' and BatchID <= a.BatchID and Attribute = 'C_INACT')
	then 'OK' else 'Mismatch' end as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'DimCustomer Gender', NULL, case when
	(select count(*) from master.DimCustomer where Gender not in ('M', 'F', 'U')) = 0
then 'OK' else 'Bad value' end, 'All Gender values are valid'
 -- FROM DUMMY TABLE

union
select 'DimCustomer age range alerts', BatchID, Result, 'Count of age range alerts matches audit table' from (
	select distinct BatchID, case when
		(select count(*) from master.DImessages where MessageType = 'Alert' and BatchID = a.BatchID and MessageText = 'DOB out of range') =
		(select Value from master.Audit where DataSet = 'DimCustomer' and BatchID = a.BatchID and Attribute = 'C_DOB_TO') +
		(select Value from master.Audit where DataSet = 'DimCustomer' and BatchID = a.BatchID and Attribute = 'C_DOB_TY')
	then 'OK' else 'Mismatch' end as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'DimCustomer customer tier alerts', BatchID, Result, 'Count of customer tier alerts matches audit table' from (
	select distinct BatchID, case when
		(select count(*) from master.DImessages where MessageType = 'Alert' and BatchID = a.BatchID and MessageText = 'Invalid customer tier') =
		(select Value from master.Audit where DataSet = 'DimCustomer' and BatchID = a.BatchID and Attribute = 'C_TIER_INV')
	then 'OK' else 'Mismatch' end as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'DimCustomer TaxID', NULL, case when (
	select count(*) from master.DimCustomer where TaxID not like '___-__-____'
	) = 0
then 'OK' else 'Mismatch' end, 'TaxID values are properly formatted'
 -- FROM DUMMY TABLE

union
select 'DimCustomer Phone1', NULL, case when (
	select count(*) from master.DimCustomer
	where Phone1 not like '+1 (___) ___-____%'
	  and Phone1 not like '(___) ___-____%'
	  and Phone1 not like '___-____%'
	  and Phone1 <> ''
	  and Phone1 is not null
	) = 0
then 'OK' else 'Mismatch' end, 'Phone1 values are properly formatted'
 -- FROM DUMMY TABLE

union
select 'DimCustomer Phone2', NULL, case when (
	select count(*) from master.DimCustomer
	where Phone2 not like '+1 (___) ___-____%'
	  and Phone2 not like '(___) ___-____%'
	  and Phone2 not like '___-____%'
	  and Phone2 <> ''
	  and Phone2 is not null
	) = 0
then 'OK' else 'Mismatch' end, 'Phone2 values are properly formatted'
 -- FROM DUMMY TABLE

union
select 'DimCustomer Phone3', NULL, case when (
	select count(*) from master.DimCustomer
	where Phone3 not like '+1 (___) ___-____%'
	  and Phone3 not like '(___) ___-____%'
	  and Phone3 not like '___-____%'
	  and Phone3 <> ''
	  and Phone3 is not null
	) = 0
then 'OK' else 'Mismatch' end, 'Phone3 values are properly formatted'
 -- FROM DUMMY TABLE

union
select 'DimCustomer Email1', NULL, case when (
	select count(*) from master.DimCustomer
	where Email1 not like '_%.%@%.%'
	  and Email1 is not null
	) = 0
then 'OK' else 'Mismatch' end, 'Email1 values are properly formatted'
 -- FROM DUMMY TABLE

union
select 'DimCustomer Email2', NULL, case when (
	select count(*) from master.DimCustomer
	where Email2 not like '_%.%@%.%'
	  and Email2 <> ''
	  and Email2 is not null
	) = 0
then 'OK' else 'Mismatch' end, 'Email2 values are properly formatted'
 -- FROM DUMMY TABLE


union
select 'DimCustomer LocalTaxRate', NULL, case when
	(select count(*) from master.DimCustomer) =
	(select count(*) from master.DimCustomer c join master.TaxRate t on c.LocalTaxRateDesc = t.TX_NAME and c.LocalTaxRate = t.TX_RATE) and
	(select count(distinct LocalTaxRateDesc) from master.DimCustomer) > 300
then 'OK' else 'Mismatch' end, 'LocalTaxRateDesc and LocalTaxRate values are from TaxRate table'
 -- FROM DUMMY TABLE

union
select 'DimCustomer NationalTaxRate', NULL, case when
	(select count(*) from master.DimCustomer) =
	(select count(*) from master.DimCustomer c join master.TaxRate t on c.NationalTaxRateDesc = t.TX_NAME and c.NationalTaxRate = t.TX_RATE) and
	(select count(distinct NationalTaxRateDesc) from master.DimCustomer) >= 9   -- Including the inequality for now, because the generated data is not sticking to national tax rates
then 'OK' else 'Mismatch' end, 'NationalTaxRateDesc and NationalTaxRate values are from TaxRate table'
 -- FROM DUMMY TABLE

union
select 'DimCustomer demographic fields', NULL, case when
	(
		select count(*) from master.DimCustomer c
		join master.Prospect p on upper(c.FirstName || c.LastName || c.AddressLine1 || COALESCE(c.AddressLine2,'') || c.PostalCode)
						 = upper(p.FirstName || p.LastName || p.AddressLine1 || COALESCE(p.AddressLine2,'') || p.PostalCode)
	                    and COALESCE(c.CreditRating,0) = COALESCE(p.CreditRating,0) and COALESCE(c.NetWorth,0) = COALESCE(p.NetWorth,0) and COALESCE(c.MarketingNameplate, '') = COALESCE(p.MarketingNameplate,'')
-- 					    and c.IsCurrent = 1
	                    and c.IsCurrent = TRUE
	) = (
-- 		select count(*) from master.DimCustomer where AgencyID is not null and IsCurrent = 1
	    select count(*) from master.DimCustomer where AgencyID is not null and IsCurrent = TRUE
	)
then 'OK' else 'Mismatch' end, 'For current customer records that match Prospect records, the demographic fields also match'
 -- FROM DUMMY TABLE

--
-- Checks against the DimSecurity table.
--

union
select 'DimSecurity row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
	select distinct BatchID, case when
		cast((select MessageData from master.DImessages where MessageType = 'Validation' and BatchID = a.BatchID and MessageSource = 'DimSecurity' and MessageText = 'Row count') as bigint) >=
		(select sum(Value) from master.Audit where DataSet = 'DimSecurity' and Attribute = 'FW_SEC' and BatchID <= a.BatchID)
	then 'OK' else 'Too few rows' end as Result
	from master.Audit a where BatchID in (1)
) o

union
select 'DimSecurity distinct keys', NULL, case when
	(select count(distinct SK_SecurityID) from master.DimSecurity) =
	(select count(*) from master.DimSecurity)
then 'OK' else 'Not unique' end, 'All SKs are distinct'
 -- FROM DUMMY TABLE

-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimSecurity EndDate' checks that effective and end dates line up
--   'DimSecurity Overlap' checks that there are not records that overlap in time
--   'DimSecurity End of Time' checks that every company has a final record that goes to 9999-12-31

union
select 'DimSecurity EndDate', NULL, case when
	(select count(*) from master.DimSecurity) =
	(select count(*) from master.DimSecurity a join master.DimSecurity b on a.Symbol = b.Symbol and a.EndDate = b.EffectiveDate) +
	(select count(*) from master.DimSecurity where EndDate = '9999-12-31')
then 'OK' else 'Dates not aligned' end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
 -- FROM DUMMY TABLE

union
select 'DimSecurity Overlap', NULL, case when (
	select count(*)
	from master.DimSecurity a
	join master.DimSecurity b on a.Symbol = b.Symbol and a.SK_SecurityID <> b.SK_SecurityID and a.EffectiveDate >= b.EffectiveDate and a.EffectiveDate < b.EndDate
) = 0
then 'OK' else 'Dates overlap' end, 'Date ranges do not overlap for a given company'
 -- FROM DUMMY TABLE

union
select 'DimSecurity End of Time', NULL, case when
	(select count(distinct Symbol) from master.DimSecurity) =
	(select count(*) from master.DimSecurity where EndDate = '9999-12-31')
then 'OK' else 'End of tome not reached' end, 'Every company has one record with a date range reaching the end of time'
 -- FROM DUMMY TABLE

union
select 'DimSecurity consolidation', NULL, case when
	(select count(*) from master.DimSecurity where EffectiveDate = EndDate) = 0
then 'OK' else 'Not consolidated' end, 'No records become effective and end on the same day'
 -- FROM DUMMY TABLE

union
select 'DimSecurity batches', NULL, case when
	(select count(distinct BatchID) from master.DimSecurity) = 1 and
	(select max(BatchID) from master.DimSecurity) = 1
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
 -- FROM DUMMY TABLE

union
select 'DimSecurity IsCurrent', NULL, case when
	(select count(*) from master.DimSecurity) =
-- 	(select count(*) from master.DimSecurity where EndDate = '9999-12-31' and IsCurrent = 1) +
-- 	(select count(*) from master.DimSecurity where EndDate < '9999-12-31' and IsCurrent = 0)
    (select count(*) from master.DimSecurity where EndDate = '9999-12-31' and IsCurrent = TRUE) +
	(select count(*) from master.DimSecurity where EndDate < '9999-12-31' and IsCurrent = FALSE)
then 'OK' else 'Not current' end, 'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
 -- FROM DUMMY TABLE

union
select 'DimSecurity EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
	select distinct BatchID, (
		case when (
			select count(*) from master.DimSecurity
			where BatchID = a.BatchID and (
				EffectiveDate < (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
				EffectiveDate > (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) )
		) = 0
		then 'OK' else 'Data out of range' end
	) as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'DimSecurity Status', NULL, case when
	(select count(*) from master.DimSecurity where Status not in ('Active', 'Inactive')) = 0
then 'OK' else 'Bad value' end, 'All Status values are valid'
 -- FROM DUMMY TABLE

union
select 'DimSecurity SK_CompanyID', NULL, case when
	(select count(*) from master.DimSecurity) =
	(select count(*) from master.DimSecurity a
	 join master.DimCompany c on a.SK_CompanyID = c.SK_CompanyID and c.EffectiveDate <= a.EffectiveDate and a.EndDate <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
 -- FROM DUMMY TABLE

union
select 'DimSecurity ExchangeID', NULL, case when
	(select count(*) from master.DimSecurity where ExchangeID not in ('NYSE', 'NASDAQ', 'AMEX', 'PCX')) = 0
then 'OK' else 'Bad value - see ticket #65' end, 'All ExchangeID values are valid'
 -- FROM DUMMY TABLE

union
select 'DimSecurity Issue', NULL, case when
	(select count(*) from master.DimSecurity where Issue not in ('COMMON', 'PREF_A', 'PREF_B', 'PREF_C', 'PREF_D')) = 0
then 'OK' else 'Bad value - see ticket #65' end, 'All Issue values are valid'
 -- FROM DUMMY TABLE

--
-- Checks against the DimCompany table.
--
union
select 'DimCompany row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
	select distinct BatchID, case when
		cast((select MessageData from master.DImessages where MessageType='Validation' and BatchID = a.BatchID and MessageSource = 'DimCompany' and MessageText = 'Row count') as bigint) <=
		(select sum(Value) from master.Audit where DataSet = 'DimCompany' and Attribute = 'FW_CMP' and BatchID <= a.BatchID)
	then 'OK' else 'Too few rows' end as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o


union
select 'DimCompany distinct keys', NULL, case when
	(select count(distinct SK_CompanyID) from master.DimCompany) =
	(select count(*) from master.DimCompany)
then 'OK' else 'Not unique' end, 'All SKs are distinct'
 -- FROM DUMMY TABLE


-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimCompany EndDate' checks that effective and end dates line up
--   'DimCompany Overlap' checks that there are not records that overlap in time
--   'DimCompany End of Time' checks that every company has a final record that goes to 9999-12-31

union
select 'DimCompany EndDate', NULL, case when
	(select count(*) from master.DimCompany) =
	(select count(*) from master.DimCompany a join master.DimCompany b on a.CompanyID = b.CompanyID and a.EndDate = b.EffectiveDate) +
	(select count(*) from master.DimCompany where EndDate = '9999-12-31')
then 'OK' else 'Dates not aligned' end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
 -- FROM DUMMY TABLE

union
select 'DimCompany Overlap', NULL, case when (
	select count(*)
	from master.DimCompany a
	join master.DimCompany b on a.CompanyID = b.CompanyID and a.SK_CompanyID <> b.SK_CompanyID and a.EffectiveDate >= b.EffectiveDate and a.EffectiveDate < b.EndDate
) = 0
then 'OK' else 'Dates overlap' end, 'Date ranges do not overlap for a given company'
 -- FROM DUMMY TABLE

union
select 'DimCompany End of Time', NULL, case when
	(select count(distinct CompanyID) from master.DimCompany) =
	(select count(*) from master.DimCompany where EndDate = '9999-12-31')
then 'OK' else 'End of tome not reached' end, 'Every company has one record with a date range reaching the end of time'
 -- FROM DUMMY TABLE

union
select 'DimCompany consolidation', NULL, case when
	(select count(*) from master.DimCompany where EffectiveDate = EndDate) = 0
then 'OK' else 'Not consolidated' end, 'No records become effective and end on the same day'
-- FROM DUMMY TABLE

union
select 'DimCompany batches', NULL, case when
	(select count(distinct BatchID) from master.DimCompany) = 1 and
	(select max(BatchID) from master.DimCompany) = 1
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
-- FROM DUMMY TABLE

union
select 'DimCompany EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
	select distinct BatchID, (
		case when (
			select count(*) from master.DimCompany
			where BatchID = a.BatchID and (
				EffectiveDate < (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
				EffectiveDate > (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) )
		) = 0
		then 'OK' else 'Data out of range - see ticket #71' end
	) as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'DimCompany Status', NULL, case when
	(select count(*) from master.DimCompany where Status not in ('Active', 'Inactive')) = 0
then 'OK' else 'Bad value' end, 'All Status values are valid'
-- FROM DUMMY TABLE

union
select 'DimCompany distinct names', NULL, case when	(
	select count(*)
	from master.DimCompany a
	join master.DimCompany b on a.Name = b.Name and a.CompanyID <> b.CompanyID
) = 0
then 'OK' else 'Mismatch' end, 'Every company has a unique name'
-- FROM DUMMY TABLE

union				-- Curious, there are duplicate industry names in Industry table.  Should there be?  That's why the distinct stuff...
select 'DimCompany Industry', NULL, case when
	(select count(*) from master.DimCompany) =
	(select count(*) from master.DimCompany where Industry in (select distinct IN_NAME from master.Industry))
then 'OK' else 'Bad value' end, 'Industry values are from the Industry table'
-- FROM DUMMY TABLE

union
select 'DimCompany SPrating', NULL, case when (
	select count(*) from master.DimCompany
	where SPrating not in (	'AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+','AA-','A-','BBB-','BB-','B-','CCC-' )
	  and SPrating is not null
) = 0
then 'OK' else 'Bad value' end, 'All SPrating values are valid'
-- FROM DUMMY TABLE

union			-- Right now we have blank (but not null) country names.  Should there be?
select 'DimCompany Country', NULL, case when (
	select count(*) from master.DimCompany
	where Country not in ( 'Canada', 'United States of America', '' )
	  and Country is not null
) = 0
then 'OK' else 'Bad value' end, 'All Country values are valid'
-- FROM DUMMY TABLE

--
-- Checks against the Prospect table.
--
union
select 'Prospect SK_UpdateDateID', NULL, case when
	(select count(*) from master.Prospect where SK_RecordDateID < SK_UpdateDateID) = 0
then 'OK' else 'Mismatch' end, 'SK_RecordDateID must be newer or same as SK_UpdateDateID'
 -- FROM DUMMY TABLE

union
select 'Prospect SK_RecordDateID', BatchID, Result, 'All records from batch have SK_RecordDateID in or after the batch time window' from (
	select distinct BatchID, (
		case when (
			select count(*) from master.Prospect p
			where p.BatchID = a.BatchID and (
				(select DateValue from master.DimDate where SK_DateID = p.SK_RecordDateID) <
				(select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
			)
		) = 0
		then 'OK' else 'Mismatch' end
	) as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'Prospect batches', NULL, case when
	(select count(distinct BatchID) from master.Prospect) = 3 and
	(select max(BatchID) from master.Prospect) = 3
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
 -- FROM DUMMY TABLE

 union
select 'Prospect Country', NULL, case when (
	select count(*) from master.Prospect
	where Country not in ( 'Canada', 'United States of America' ) -- For the tiny sample data it would be ( 'CANADA', 'USA' )
	  and Country is not null
) = 0
then 'OK' else 'Bad value' end, 'All Country values are valid'
 -- FROM DUMMY TABLE

union
select 'Prospect MarketingNameplate', NULL, case when (
	select sum(case when (COALESCE(NetWorth,0) > 1000000 or COALESCE(Income,0) > 200000) and MarketingNameplate not like '%HighValue%' then 1 else 0 end) +
	       sum(case when (COALESCE(NumberChildren,0) > 3 or COALESCE(NumberCreditCards,0) > 5) and MarketingNameplate not like '%Expenses%' then 1 else 0 end) +
	       sum(case when (COALESCE(Age,0) > 45) and MarketingNameplate not like '%Boomer%' then 1 else 0 end) +
	       sum(case when (COALESCE(Income,50000) < 50000 or COALESCE(CreditRating,600) < 600 or COALESCE(NetWorth,100000) < 100000) and MarketingNameplate not like '%MoneyAlert%' then 1 else 0 end) +
	       sum(case when (COALESCE(NumberCars,0) > 3 or COALESCE(NumberCreditCards,0) > 7) and MarketingNameplate not like '%Spender%' then 1 else 0 end) +
	       sum(case when (COALESCE(Age,25) < 25 and COALESCE(NetWorth,0) > 1000000) and MarketingNameplate not like '%Inherited%' then 1 else 0 end) +
		   sum(case when COALESCE(MarketingNameplate, '') not in (   -- Technically, a few of these combinations cannot really happen
				'','HighValue','Expenses','HighValue+Expenses','Boomer','HighValue+Boomer','Expenses+Boomer','HighValue+Expenses+Boomer','MoneyAlert','HighValue+MoneyAlert',
				'Expenses+MoneyAlert','HighValue+Expenses+MoneyAlert','Boomer+MoneyAlert','HighValue+Boomer+MoneyAlert','Expenses+Boomer+MoneyAlert','HighValue+Expenses+Boomer+MoneyAlert',
				'Spender','HighValue+Spender','Expenses+Spender','HighValue+Expenses+Spender','Boomer+Spender','HighValue+Boomer+Spender','Expenses+Boomer+Spender',
				'HighValue+Expenses+Boomer+Spender','MoneyAlert+Spender','HighValue+MoneyAlert+Spender','Expenses+MoneyAlert+Spender','HighValue+Expenses+MoneyAlert+Spender',
				'Boomer+MoneyAlert+Spender','HighValue+Boomer+MoneyAlert+Spender','Expenses+Boomer+MoneyAlert+Spender','HighValue+Expenses+Boomer+MoneyAlert+Spender','Inherited',
				'HighValue+Inherited','Expenses+Inherited','HighValue+Expenses+Inherited','Boomer+Inherited','HighValue+Boomer+Inherited','Expenses+Boomer+Inherited',
				'HighValue+Expenses+Boomer+Inherited','MoneyAlert+Inherited','HighValue+MoneyAlert+Inherited','Expenses+MoneyAlert+Inherited','HighValue+Expenses+MoneyAlert+Inherited',
				'Boomer+MoneyAlert+Inherited','HighValue+Boomer+MoneyAlert+Inherited','Expenses+Boomer+MoneyAlert+Inherited','HighValue+Expenses+Boomer+MoneyAlert+Inherited',
				'Spender+Inherited','HighValue+Spender+Inherited','Expenses+Spender+Inherited','HighValue+Expenses+Spender+Inherited','Boomer+Spender+Inherited',
				'HighValue+Boomer+Spender+Inherited','Expenses+Boomer+Spender+Inherited','HighValue+Expenses+Boomer+Spender+Inherited','MoneyAlert+Spender+Inherited',
				'HighValue+MoneyAlert+Spender+Inherited','Expenses+MoneyAlert+Spender+Inherited','HighValue+Expenses+MoneyAlert+Spender+Inherited','Boomer+MoneyAlert+Spender+Inherited',
				'HighValue+Boomer+MoneyAlert+Spender+Inherited','Expenses+Boomer+MoneyAlert+Spender+Inherited','HighValue+Expenses+Boomer+MoneyAlert+Spender+Inherited'
				) then 1 else 0  end)
	from master.Prospect
) = 0
then 'OK' else 'Bad value' end, 'All MarketingNameplate values match the data'
 -- FROM DUMMY TABLE



--
-- Checks against the FactWatches table.
--

union
select 'FactWatches row count', BatchID, Result, 'Actual row count matches Audit table' from (
	select distinct BatchID, (
		case when
			cast((select MessageData from master.DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) as int) -
			cast((select MessageData from master.DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID-1) as int) =
			(select Value from master.Audit where DataSet = 'FactWatches' and Attribute = 'WH_ACTIVE' and BatchID = a.BatchID)
		then 'OK' else 'Mismatch' end
	) as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'FactWatches batches', NULL, case when
	(select count(distinct BatchID) from master.FactWatches) = 3  and
	(select max(BatchID) from master.FactWatches) = 3
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
 -- FROM DUMMY TABLE

union
select 'FactWatches active watches', BatchID, Result, 'Actual total matches Audit table' from (
	select distinct BatchID, case when
		(select cast(MessageData as bigint) from master.DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) +
		(select cast(MessageData as bigint) from master.DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Inactive watches' and BatchID = a.BatchID) =
		(select sum(Value) from master.Audit where DataSet = 'FactWatches' and Attribute = 'WH_RECORDS' and BatchID <= a.BatchID)
	then 'OK' else 'Mismatch' end as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'FactWatches SK_CustomerID', NULL, case when
	(select count(*) from master.FactWatches) =
	(select count(*) from master.FactWatches a
	 join master.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID
	  and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID_DatePlaced)
	  and (select DateValue from master.DimDate where SK_DateID = a.SK_DateID_DatePlaced) <= c.EndDate
	  )
then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
 -- FROM DUMMY TABLE

union
select 'FactWatches SK_SecurityID', NULL, case when
	(select count(*) from master.FactWatches) =
	(select count(*) from master.FactWatches a
	 join master.DimSecurity c on a.SK_SecurityID = c.SK_SecurityID
	  and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID_DatePlaced)
	  and (select DateValue from master.DimDate where SK_DateID = a.SK_DateID_DatePlaced) <= c.EndDate )
then 'OK' else 'Bad join' end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
 -- FROM DUMMY TABLE

-- union
-- select 'FactWatches date check', BatchID, Result, 'All SK_DateID_ values are in the correct batch time window' from (
-- 	select distinct BatchID, (
-- 		case when (
-- 			select count(*) from master.FactWatches w
-- 			where w.BatchID = a.BatchID and (
-- 				w.SK_DateID_DateRemoved is null and (
-- 					(select DateValue from master.DimDate where SK_DateID = w.SK_DateID_DatePlaced) >
-- 					(select Date from master.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
-- 					or
-- 					(select DateValue from master.DimDate where SK_DateID = w.SK_DateID_DatePlaced) <
-- 					(select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
-- 				) or
-- 				w.SK_DateID_DateRemoved is not null and (
-- 					(select DateValue from master.DimDate where SK_DateID = w.SK_DateID_DateRemoved) >
-- 					(select Date from master.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
-- 					or
-- 					(select DateValue from master.DimDate where SK_DateID = w.SK_DateID_DateRemoved) <
-- 					(select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
-- 					or
-- 					SK_DateID_DatePlaced > SK_DateID_DateRemoved
-- 				)
-- 			)
-- 		) = 0
-- 		then 'OK' else 'Mismatch' end
-- 	) as Result
-- 	from master.Audit a where BatchID in (1, 2, 3)
-- ) o


--
-- Checks against the DimTrade table.
--

union
select 'DimTrade row count', BatchID, Result, 'Actual total matches Audit table' from (
	select distinct BatchID, case when
-- 		(select MessageData from master.DImessages where MessageSource = 'DimTrade' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID)  =
-- 		(select sum(Value) from master.Audit where DataSet = 'DimTrade' and Attribute = 'T_NEW' and BatchID <= a.BatchID)
	    (select MessageData from master.DImessages where MessageSource = 'DimTrade' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID)  =
		(select sum(Value)::text from master.Audit where DataSet = 'DimTrade' and Attribute = 'T_NEW' and BatchID <= a.BatchID)
	then 'OK' else 'Mismatch' end as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'DimTrade canceled trades', NULL, case when
		(select count(*) from master.DimTrade where Status = 'Canceled')  =
		(select sum(Value) from master.Audit where DataSet = 'DimTrade' and Attribute = 'T_CanceledTrades')
then 'OK' else 'Mismatch' end, 'Actual row counts matches Audit table'
 -- FROM DUMMY TABLE

union
select 'DimTrade commission alerts', NULL, case when
		(select count(*) from master.DImessages where MessageType = 'Alert' and messageText = 'Invalid trade commission')  =
		(select sum(Value) from master.Audit where DataSet = 'DimTrade' and Attribute = 'T_InvalidCommision')
then 'OK' else 'Mismatch' end, 'Actual row counts matches Audit table'
 -- FROM DUMMY TABLE

union
select 'DimTrade charge alerts', NULL, case when
		(select count(*) from master.DImessages where MessageType = 'Alert' and messageText = 'Invalid trade fee')  =
		(select sum(Value) from master.Audit where DataSet = 'DimTrade' and Attribute = 'T_InvalidCharge')
then 'OK' else 'Mismatch' end, 'Actual row counts matches Audit table'
 -- FROM DUMMY TABLE

union
select 'DimTrade batches', NULL, case when
	(select count(distinct BatchID) from master.DimTrade) = 3 and
	(select max(BatchID) from master.DimTrade) = 3
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
 -- FROM DUMMY TABLE

union
select 'DimTrade distinct keys', NULL, case when
	(select count(distinct TradeID) from master.DimTrade) =
	(select count(*) from master.DimTrade)
then 'OK' else 'Not unique' end, 'All keys are distinct'
 -- FROM DUMMY TABLE

union
select 'DimTrade SK_BrokerID', NULL, case when
	(select count(*) from master.DimTrade) =
	(select count(*) from master.DimTrade a
	 join master.DimBroker c on a.SK_BrokerID = c.SK_BrokerID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_BrokerIDs match a DimBroker record with a valid date range'
 -- FROM DUMMY TABLE

union
select 'DimTrade SK_CompanyID', NULL, case when
	(select count(*) from master.DimTrade) =
	(select count(*) from master.DimTrade a
	 join master.DimCompany c on a.SK_CompanyID = c.SK_CompanyID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
 -- FROM DUMMY TABLE

union
select 'DimTrade SK_SecurityID', NULL, case when
	(select count(*) from master.DimTrade) =
	(select count(*) from master.DimTrade a
	 join master.DimSecurity c on a.SK_SecurityID = c.SK_SecurityID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
 -- FROM DUMMY TABLE

union
select 'DimTrade SK_CustomerID', NULL, case when
	(select count(*) from master.DimTrade) =
	(select count(*) from master.DimTrade a
	 join master.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
 -- FROM DUMMY TABLE

union
select 'DimTrade SK_AccountID', NULL, case when
	(select count(*) from master.DimTrade) =
	(select count(*) from master.DimTrade a
	 join master.DimAccount c on a.SK_AccountID = c.SK_AccountID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from master.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_AccountIDs match a DimAccount record with a valid date range'
 -- FROM DUMMY TABLE

-- union
-- select 'DimTrade date check', BatchID, Result, 'All SK_DateID values are in the correct batch time window' from (
-- 	select distinct BatchID, (
-- 		case when (
-- 			select count(*) from master.DimTrade w
-- 			where w.BatchID = a.BatchID and (
-- 				w.SK_CloseDateID is null and (
-- 					(select DateValue from master.DimDate where SK_DateID = w.SK_CreateDateID) >
-- 					(select Date from master.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
-- 					or
-- 					(select DateValue from master.DimDate where SK_DateID = w.SK_CreateDateID) <
-- 					(case when w.Type like 'Limit%'  /* Limit trades can have create dates earlier than the current Batch date, but not earlier than Batch1's first date */
--                                               then (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = 1)
--                                               else (select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) end)
-- 				) or
-- 				w.SK_CloseDateID is not null and (
-- 					(select DateValue from master.DimDate where SK_DateID = w.SK_CloseDateID) >
-- 					(select Date from master.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
-- 					or
-- 					(select DateValue from master.DimDate where SK_DateID = w.SK_CloseDateID) <
-- 					(select Date from master.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
-- 					or
-- 					SK_CloseDateID < SK_CreateDateID
-- 				)
-- 			)
-- 		) = 0
-- 		then 'OK' else 'Mismatch' end
-- 	) as Result
-- 	from master.Audit a where BatchID in (1, 2, 3)
-- ) o

union
select 'DimTrade Status', NULL, case when (
	select count(*) from master.DimTrade
	where Status not in ( 'Canceled', 'Pending', 'Submitted', 'Active', 'Completed' )
) = 0
then 'OK' else 'Bad value' end, 'All Trade Status values are valid'
 -- FROM DUMMY TABLE

union
select 'DimTrade Type', NULL, case when (
	select count(*) from master.DimTrade
	where Type not in ( 'Market Buy', 'Market Sell', 'Stop Loss', 'Limit Sell', 'Limit Buy' )
) = 0
then 'OK' else 'Bad value' end, 'All Trade Type values are valid'
 -- FROM DUMMY TABLE


--
-- Checks against the Financial table.
--

union
select 'Financial row count', NULL, case when
-- 		(select MessageData from master.DImessages where MessageSource = 'Financial' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = 1)  =
-- 		(select sum(Value) from master.Audit where DataSet = 'Financial' and Attribute = 'FW_FIN')
        (select MessageData from master.DImessages where MessageSource = 'Financial' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = 1)  =
		(select sum(Value)::text from master.Audit where DataSet = 'Financial' and Attribute = 'FW_FIN')
then 'OK' else 'Mismatch' end, 'Actual row count matches Audit table'
 -- FROM DUMMY TABLE

union
select 'Financial SK_CompanyID', NULL, case when
	(select count(*) from master.Financial) =
	(select count(*) from master.Financial a join master.DimCompany c on a.SK_CompanyID = c.SK_CompanyID )
then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record'
 -- FROM DUMMY TABLE

union
select 'Financial FI_YEAR', NULL, case when (
-- 	(select count(*) from master.Financial where FI_YEAR < year((select Date from master.Audit where DataSet = 'Batch' and BatchID = 1 and Attribute = 'FirstDay'))) +
-- 	(select count(*) from master.Financial where FI_YEAR > year((select Date from master.Audit where DataSet = 'Batch' and BatchID = 1 and Attribute = 'LastDay')))
    (select count(*) from master.Financial where FI_YEAR < date_part('year', (select Date from master.Audit where DataSet = 'Batch' and BatchID = 1 and Attribute = 'FirstDay'))) +
	(select count(*) from master.Financial where FI_YEAR > date_part('year', (select Date from master.Audit where DataSet = 'Batch' and BatchID = 1 and Attribute = 'LastDay')))
) = 0
then 'OK' else 'Bad Year' end, 'All Years are within Batch1 range'
 -- FROM DUMMY TABLE

union
select 'Financial FI_QTR', NULL, case when (
	select count(*) from master.Financial where FI_QTR not in ( 1, 2, 3, 4 )
) = 0
then 'OK' else 'Bad Qtr' end, 'All quarters are in ( 1, 2, 3, 4 )'
 -- FROM DUMMY TABLE

union
select 'Financial FI_QTR_START_DATE', NULL, case when (
	select count(*) from master.Financial
-- 	where FI_YEAR <> year(FI_QTR_START_DATE)
-- 	   or month(FI_QTR_START_DATE) <> (FI_QTR-1)*3+1
-- 	   or day(FI_QTR_START_DATE) <> 1
    where FI_YEAR <> date_part('year', FI_QTR_START_DATE)
	   or date_part('month', FI_QTR_START_DATE) <> (FI_QTR-1)*3+1
	   or date_part('day', FI_QTR_START_DATE) <> 1
) = 0
then 'OK' else 'Bad date' end, 'All quarters start on correct date'
 -- FROM DUMMY TABLE

union
select 'Financial EPS', NULL, case when (
	select count(*) from master.Financial
	where Round(FI_NET_EARN/FI_OUT_BASIC,2)- FI_BASIC_EPS not between -0.4 and 0.4
	   or  Round(FI_NET_EARN/FI_OUT_DILUT,2) - FI_DILUT_EPS not between -0.4 and 0.4
	   or  Round(FI_NET_EARN/FI_REVENUE,2) - FI_MARGIN not between -0.4 and 0.4
) = 0
then 'OK' else 'Bad EPS' end, 'Earnings calculations are valid'
 -- FROM DUMMY TABLE


--
-- Checks against the FactMarketHistory table.
--

union
select 'FactMarketHistory row count', BatchID, Result, 'Actual row count matches Audit table' from (
	select distinct BatchID, (
		case when
			cast((select MessageData from master.DImessages where MessageSource = 'FactMarketHistory' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) as int) -
			cast((select MessageData from master.DImessages where MessageSource = 'FactMarketHistory' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID-1) as int) =
			(select Value from master.Audit where DataSet = 'FactMarketHistory' and Attribute = 'DM_RECORDS' and BatchID = a.BatchID)
		then 'OK' else 'Mismatch' end
	) as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'FactMarketHistory batches', NULL, case when
	(select count(distinct BatchID) from master.FactMarketHistory) = 3 and
	(select max(BatchID) from master.FactMarketHistory) = 3
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
-- FROM DUMMY TABLE

-- Very slow procedure ahead:
union
select 'FactMarketHistory SK_CompanyID', NULL, case when
	(select count(*) from master.FactMarketHistory) =
	(select count(*) from master.FactMarketHistory a join master.DimCompany c on a.SK_CompanyID = c.SK_CompanyID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID) and (select DateValue from master.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
-- FROM DUMMY TABLE

union
select 'FactMarketHistory SK_SecurityID', NULL, case when
	(select count(*) from master.FactMarketHistory) =
	(select count(*) from master.FactMarketHistory a join master.DimSecurity c on a.SK_SecurityID = c.SK_SecurityID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID) and (select DateValue from master.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
-- FROM DUMMY TABLE

-- union
-- select 'FactMarketHistory SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
-- 	select distinct BatchID, (
-- 		case when (
-- -- 			(select count(*) from master.FactMarketHistory m where m.BatchID = a.BatchID and (select DateValue from master.DimDate where SK_DateID = m.SK_DateID) < (select Date-1 day from master.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'FirstDay')) +
-- -- 			(select count(*) from master.FactMarketHistory m where m.BatchID = a.BatchID and (select DateValue from master.DimDate where SK_DateID = m.SK_DateID) >= (select Date from master.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'LastDay'))
-- 		    (select count(*) from master.FactMarketHistory m where m.BatchID = a.BatchID and (select DateValue from master.DimDate where SK_DateID = m.SK_DateID) < (select Date - INTERVAL '1 day' from master.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'FirstDay')) +
-- 			(select count(*) from master.FactMarketHistory m where m.BatchID = a.BatchID and (select DateValue from master.DimDate where SK_DateID = m.SK_DateID) >= (select Date from master.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'LastDay'))
-- 		) = 0
-- 		then 'OK' else 'Bad Date' end
-- 	) as Result
-- 	from master.Audit a where BatchID in (1, 2, 3)
-- ) o

union
select 'FactMarketHistory relative dates', NULL, case when (
	select count(*) from master.FactMarketHistory
	where FiftyTwoWeekLow > DayLow
	   or DayLow > ClosePrice
	   or ClosePrice > DayHigh
	   or DayHigh > FiftyTwoWeekHigh
) = 0
then 'OK' else 'Bad Date' end, '52-week-low <= day_low <= close_price <= day_high <= 52-week-high'
-- FROM DUMMY TABLE

--
-- Checks against the FactHoldings table.
--
union
select 'FactHoldings row count', BatchID, Result, 'Actual row count matches Audit table' from (
	select distinct BatchID, (
		case when
			cast((select MessageData from master.DImessages where MessageSource = 'FactHoldings' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) as int) -
			cast((select MessageData from master.DImessages where MessageSource = 'FactHoldings' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID-1) as int) =
			(select Value from master.Audit where DataSet = 'FactHoldings' and Attribute = 'HH_RECORDS' and BatchID = a.BatchID)
		then 'OK' else 'Mismatch' end
	) as Result
	from master.Audit a where BatchID in (1, 2, 3)
) o

union
select 'FactHoldings batches', NULL, case when
	(select count(distinct BatchID) from master.FactHoldings) = 3 and
	(select max(BatchID) from master.FactHoldings) = 3
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
-- FROM DUMMY TABLE

union
/* It is possible that the dimension record has changed between orgination of the trade and the completion of the trade. *
 * So, we can check that the Effective Date of the dimension record is older than the the completion date, but the end date could be earlier or later than the completion date
 */
select 'FactHoldings SK_CustomerID', NULL, case when
	(select count(*) from master.FactHoldings) =
	(select count(*) from master.FactHoldings a join master.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID)
       )
then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
-- FROM DUMMY TABLE

union
select 'FactHoldings SK_AccountID', NULL, case when
	(select count(*) from master.FactHoldings) =
	(select count(*) from master.FactHoldings a join master.DimAccount c on a.SK_AccountID = c.SK_AccountID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID)
       )
then 'OK' else 'Bad join' end, 'All SK_AccountIDs match a DimAccount record with a valid date range'
-- FROM DUMMY TABLE

union
select 'FactHoldings SK_CompanyID', NULL, case when
	(select count(*) from master.FactHoldings) =
	(select count(*) from master.FactHoldings a join master.DimCompany c on a.SK_CompanyID = c.SK_CompanyID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID)
       )
then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
-- FROM DUMMY TABLE

union
select 'FactHoldings SK_SecurityID', NULL, case when
	(select count(*) from master.FactHoldings) =
	(select count(*) from master.FactHoldings a join master.DimSecurity c on a.SK_SecurityID = c.SK_SecurityID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID)
       )
then 'OK' else 'Bad join' end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
-- FROM DUMMY TABLE

union
select 'FactHoldings CurrentTradeID', NULL, case when
	(select count(*) from master.FactHoldings) =
	(select count(*) from master.FactHoldings a join master.DimTrade t on a.CurrentTradeID = t.TradeID and a.SK_DateID = t.SK_CloseDateID and a.SK_TimeID = t.SK_CloseTimeID)
then 'OK' else 'Failed' end, 'CurrentTradeID matches a DimTrade record with and Close Date and Time are values are used as the holdings date and time'
-- FROM DUMMY TABLE

-- union
-- select 'FactHoldings SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
-- 	select distinct BatchID, (
-- 		case when (
-- 			(select count(*) from master.FactHoldings m where BatchID = a.BatchID and (select DateValue from master.DimDate where SK_DateID = m.SK_DateID) < (select Date from master.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'FirstDay')) +
-- 			(select count(*) from master.FactHoldings m where BatchID = a.BatchID and (select DateValue from master.DimDate where SK_DateID = m.SK_DateID) > (select Date from master.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'LastDay'))
-- 		) = 0
-- 		then 'OK' else 'Bad Date' end
-- 	) as Result
-- 	from master.Audit a where BatchID in (1, 2, 3)
-- ) o

--
-- Checks against the FactCashBalances table.
--
union
select 'FactCashBalances batches', NULL, case when
	(select count(distinct BatchID) from master.FactCashBalances) = 3 and
	(select max(BatchID) from master.FactCashBalances) = 3
then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
-- FROM DUMMY TABLE

union
select 'FactCashBalances SK_CustomerID', NULL, case when
	(select count(*) from master.FactCashBalances) =
	(select count(*) from master.FactCashBalances a join master.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID) and (select DateValue from master.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
-- FROM DUMMY TABLE

union
select 'FactCashBalances SK_AccountID', NULL, case when
	(select count(*) from master.FactCashBalances) =
	(select count(*) from master.FactCashBalances a join master.DimAccount c on a.SK_AccountID = c.SK_AccountID and c.EffectiveDate <= (select DateValue from master.DimDate where SK_DateID = a.SK_DateID) and (select DateValue from master.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
then 'OK' else 'Bad join' end, 'All SK_AccountIDs match a DimAccount record with a valid date range'
-- FROM DUMMY TABLE

-- union
-- select 'FactCashBalances SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
-- 	select distinct BatchID, (
-- 		case when (
-- 			(select count(*) from master.FactCashBalances m where BatchID = a.BatchID and (select DateValue from master.DimDate where SK_DateID = m.SK_DateID) < (select Date from master.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'FirstDay')) +
-- 			(select count(*) from master.FactCashBalances m where BatchID = a.BatchID and (select DateValue from master.DimDate where SK_DateID = m.SK_DateID) > (select Date from master.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'LastDay'))
-- 		) = 0
-- 		then 'OK' else 'Bad Date' end
-- 	) as Result
-- 	from master.Audit a where BatchID in (1, 2, 3)
-- ) o

/*
 *  Checks against the Batch Validation Query row counts
 */

union
select 'Batch row count: ' || MessageSource, BatchID,
	case when RowsLastBatch > RowsThisBatch then 'Row count decreased' else 'OK' end,
	'Row counts do not decrease between successive batches'
from (
	select distinct(a.BatchID), m.MessageSource, cast(m1.MessageData as bigint) as RowsThisBatch, cast(m2.MessageData as bigint) as RowsLastBatch
	from master.Audit a
	full join master.DImessages m on m.BatchID = 0 and m.MessageText = 'Row count' and m.MessageType = 'Validation'
	join master.DImessages m1 on m1.BatchID = a.BatchID   and m1.MessageSource = m.MessageSource and m1.MessageText = 'Row count' and m1.MessageType = 'Validation'
	join master.DImessages m2 on m2.BatchID = a.BatchID-1 and m2.MessageSource = m.MessageSource and m2.MessageText = 'Row count' and m2.MessageType = 'Validation'
	where a.BatchID in (1, 2, 3)
) o

union
select 'Batch joined row count: ' || MessageSource, BatchID,
	case when RowsJoined = RowsUnjoined then 'OK' else 'No match' end,
	'Row counts match when joined to dimensions'
from (
	select distinct(a.BatchID), m.MessageSource, cast(m1.MessageData as bigint) as RowsUnjoined, cast(m2.MessageData as bigint) as RowsJoined
	from master.Audit a
	full join master.DImessages m on m.BatchID = 0 and m.MessageText = 'Row count' and m.MessageType = 'Validation'
	join master.DImessages m1 on m1.BatchID = a.BatchID and m1.MessageSource = m.MessageSource and m1.MessageText = 'Row count' and m1.MessageType = 'Validation'
	join master.DImessages m2 on m2.BatchID = a.BatchID and m2.MessageSource = m.MessageSource and m2.MessageText = 'Row count joined' and m2.MessageType = 'Validation'
	where a.BatchID in (1, 2, 3)
) o


/*
 *  Checks against the Data Visibility Query row counts
 */

union
select 'Data visibility row counts: ' || MessageSource , NULL as BatchID,
	case when regressions = 0 then 'OK' else 'Row count decreased' end,
	'Row counts must be non-decreasing over time'
from (
	select m1.MessageSource, sum( case when cast(m1.MessageData as bigint) > cast(m2.MessageData as bigint) then 1 else 0 end ) as regressions
	from master.DImessages m1
	join master.DImessages m2 on
		m2.MessageType IN ('Visibility_1', 'Visibility_2') and
		m2.MessageText = 'Row count' and
		m2.MessageSource = m1.MessageSource and
		m2.MessageDateAndTime > m1.MessageDateAndTime
	where m1.MessageType IN ('Visibility_1', 'Visibility_2') and m1.MessageText = 'Row count'
	group by m1.MessageSource
) o

union
select 'Data visibility joined row counts: ' || MessageSource , NULL as BatchID,
	case when regressions = 0 then 'OK' else 'No match' end,
	'Row counts match when joined to dimensions'
from (
	select m1.MessageSource, sum( case when cast(m1.MessageData as bigint) > cast(m2.MessageData as bigint) then 1 else 0 end ) as regressions
	from master.DImessages m1
	join master.DImessages m2 on
		m2.MessageType = 'Visibility_1' and
		m2.MessageText = 'Row count joined' and
		m2.MessageSource = m1.MessageSource and
		m2.MessageDateAndTime = m1.MessageDateAndTime
	where m1.MessageType = 'Visibility_1' and m1.MessageText = 'Row count'
	group by m1.MessageSource
) o

/* close the outer query */
) q
order by Test, Batch
)

SELECT *, (SELECT COUNT(*) FROM audit_results WHERE result = 'OK')
FROM audit_results
ORDER BY 1, 3;

