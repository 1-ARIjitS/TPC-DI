-- de-activate most recent account versions
UPDATE master.dimaccount prev_acc SET
	iscurrent = false,
	enddate = b.batchdate
FROM staging.batchdate b, staging.account post_acc
WHERE post_acc.CDC_FLAG = 'U' and post_acc.CA_ID = prev_acc.accountid and prev_acc.iscurrent = true;


-- insert new accounts
INSERT INTO master.dimaccount

with sk_account as (
	SELECT MAX(sk_accountid) AS max_account_id
	FROM master.dimaccount
)

, account_insert as (
	SELECT  row_number() over(order by CA_ID) + sk_account.max_account_id as sk_accountid,
			CA_ID as accountid,
			sk_brokerid,
			sk_customerid,
			ST_NAME as status,
			CA_NAME as accountdesc,
			CA_TAX_ST as taxstatus,
			true as iscurrent,
			2 as batchid,
			batchdate as effectivedate,
			'9999-12-31'::date as enddate
	FROM staging.account, master.statustype, master.dimbroker, master.dimcustomer, staging.batchdate, sk_account
	WHERE CA_ST_ID = ST_ID and master.dimbroker.BrokerID = CA_B_ID and master.dimbroker.IsCurrent
	and master.dimcustomer.CustomerID = CA_C_ID and master.dimcustomer.IsCurrent
)

SELECT * from account_insert;
