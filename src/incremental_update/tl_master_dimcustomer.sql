CREATE TEMP TABLE staging_updated_customers(
	nat_id numeric(11),
	sk_id numeric(11)
);

INSERT INTO staging_updated_customers
SELECT master.dimcustomer.customerid, master.dimcustomer.sk_customerid
FROM master.dimcustomer, staging.customer
where staging.customer.CDC_FLAG = 'U' and master.dimcustomer.customerid = staging.customer.C_ID and master.dimcustomer.iscurrent = true;



-- de-activate most recent customer versions
UPDATE master.dimcustomer prev_customer SET
	iscurrent = false,
	enddate = b.batchdate
FROM staging.batchdate b, staging.customer post_customer
WHERE post_customer.CDC_FLAG = 'U' and post_customer.C_ID = prev_customer.customerid and prev_customer.iscurrent = true;


-- insert new customers
INSERT INTO master.dimcustomer

WITH sk_customer as (
	SELECT MAX(sk_customerid) AS max_customer_id
	FROM master.dimcustomer
)

, customer_insert as (
	SELECT  row_number() over(order by C_ID) + sk_customer.max_customer_id as sk_customerid,
			C_ID as customerid,
			C_TAX_ID as taxid,
			ST_NAME as status,
			C_L_NAME as lastname,
			C_F_NAME as firstname,
			C_M_NAME as middleinitial,
			case
				when C_GNDR = 'M' or C_GNDR = 'F'
				then C_GNDR
				else 'U'
			end as gender,
			C_TIER as tier,
			C_DOB as dob,
			C_ADLINE1 as addressline1,
			C_ADLINE2 as addressline2,
			C_ZIPCODE as postalcode,
			C_CITY as city,
			C_STATE_PROV as stateprov,
			C_CTRY as country,
			case
				when C_CTRY_1 is not null and C_AREA_1 is not null and C_LOCAL_1 is not null
				then '+' || C_CTRY_1 || ' (' || C_AREA_1 || ') ' || C_LOCAL_1 || coalesce(C_EXT_1, '')

				when C_CTRY_1 is null and C_AREA_1 is not null and C_LOCAL_1 is not null
				then '(' || C_AREA_1 || ') ' || C_LOCAL_1 || coalesce(C_EXT_1, '')

				when C_AREA_1 is null and C_LOCAL_1 is not null
				then C_LOCAL_1 || coalesce(C_EXT_1, '')

				else null
			end as phone1,
			case
				when C_CTRY_2 is not null and C_AREA_2 is not null and C_LOCAL_2 is not null
				then '+' || C_CTRY_2 || ' (' || C_AREA_2 || ') ' || C_LOCAL_2 || coalesce(C_EXT_2, '')

				when C_CTRY_2 is null and C_AREA_2 is not null and C_LOCAL_2 is not null
				then '(' || C_AREA_2 || ') ' || C_LOCAL_2 || coalesce(C_EXT_2, '')

				when C_AREA_2 is null and C_LOCAL_2 is not null
				then C_LOCAL_2 || coalesce(C_EXT_2, '')

				else null
			end as phone2,
			case
				when C_CTRY_3 is not null and C_AREA_3 is not null and C_LOCAL_3 is not null
				then '+' || C_CTRY_3 || ' (' || C_AREA_3 || ') ' || C_LOCAL_3 || coalesce(C_EXT_3, '')

				when C_CTRY_3 is null and C_AREA_3 is not null and C_LOCAL_3 is not null
				then '(' || C_AREA_3 || ') ' || C_LOCAL_3 || coalesce(C_EXT_3, '')

				when C_AREA_3 is null and C_LOCAL_3 is not null
				then C_LOCAL_3 || coalesce(C_EXT_3, '')

				else null
			end as phone3,
			C_EMAIL_1 as email1,
			C_EMAIL_2 as email2,
			natrate.TX_NAME as nationaltaxratedesc,
			natrate.TX_RATE as nationaltaxrate,
			locrate.TX_NAME as localtaxratedesc,
			locrate.TX_RATE as localtaxrate,
			pros.agencyid as agencyid,
			pros.creditrating as creditrating,
			pros.networth as networth,
			pros.marketingnameplate as marketingnameplate,
			true as iscurrent,
			2 as batchid,
			batchdate as effectivedate,
			'9999-12-31'::date as enddate
	FROM staging.customer, master.statustype, master.taxrate natrate, master.taxrate locrate, staging.batchdate, sk_customer, master.prospect pros
	WHERE C_ST_ID = ST_ID and C_NAT_TX_ID = natrate.TX_ID and C_LCL_TX_ID = locrate.TX_ID and
			C_F_NAME = pros.FirstName and
			C_L_NAME = pros.LastName and
			C_ADLINE1 = pros.AddressLine1 and
			C_ADLINE2 = pros.AddressLine2 and
			C_ZIPCODE = pros.PostalCode
)

SELECT * from customer_insert;

UPDATE master.dimaccount SET
    sk_customerid = master.dimcustomer.sk_customerid
FROM master.dimcustomer, staging_updated_customers, master.dimaccount da
where master.dimcustomer.customerid = staging_updated_customers.nat_id and master.dimcustomer.iscurrent = true
	and da.sk_customerid = staging_updated_customers.sk_id and da.IsCurrent = true;
