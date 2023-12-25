DROP TABLE inserted_row_counts;

with inserted_rows as(
insert into master.prospect
with prospect_prior as (
    SELECT
        agencyid,
        batchid,
				sk_updatedateid,
        lastname,
        firstname,
        middleinitial,
        gender,
        addressline1,
        addressline2,
        postalcode,
        city,
        state,
        country,
        phone,
        income,
        numbercars,
        numberchildren,
        maritalstatus,
        age,
        creditrating,
        ownorrentflag,
        employer,
        numbercreditcards,
        networth
    FROM master.prospect
    WHERE sk_updatedateid IS NOT NULL
),

prior_date as(
select prospect_prior.agencyid as agencyid, prospect_prior.sk_updatedateid as sk_updatedateid from
staging.prospect as p
inner join prospect_prior
on prospect_prior.agencyid = p.agencyid
and p.lastname = prospect_prior.lastname
AND p.firstname = prospect_prior.firstname
AND p.middleinitial = prospect_prior.middleinitial
AND p.gender = prospect_prior.gender
AND p.addressline1 = prospect_prior.addressline1
AND p.addressline2 = prospect_prior.addressline2
AND p.postalcode = prospect_prior.postalcode
AND p.city = prospect_prior.city
AND p.state = prospect_prior.state
AND p.country = prospect_prior.country
AND p.phone = prospect_prior.phone
AND p.income = prospect_prior.income
AND p.numbercars = prospect_prior.numbercars
AND p.numberchildren = prospect_prior.numberchildren
AND p.maritalstatus = prospect_prior.maritalstatus
AND p.age = prospect_prior.age
AND p.creditrating = prospect_prior.creditrating
AND p.ownorrentflag = prospect_prior.ownorrentflag
AND p.employer = prospect_prior.employer
AND p.numbercreditcards = prospect_prior.numbercreditcards
AND p.networth = prospect_prior.networth
),

batchdate as (
  select sk_dateid
  from master.dimdate
  where datevalue = (select batchdate from staging.batchdate)
)

	select
	  p.agencyid
	, (select sk_dateid from batchdate)
	, case
	  when (select count(*) from prior_date where p.agencyid= prior_date.agencyid)>0
		then (select distinct sk_updatedateid from prior_date where p.agencyid= prior_date.agencyid)
		else (select sk_dateid from batchdate)
		end
	, 3 as batchid
	, false
	, p.lastname
	, p.firstname
	, p.middleinitial
	, p.gender
	, p.addressline1
	, p.addressline2
	, p.postalcode
	, p.city
	, p.state
	, p.country
	, p.phone
	, p.income
	, p.numbercars
	, p.numberchildren
	, p.maritalstatus
	, p.age
	, p.creditrating
	, p.ownorrentflag
	, p.employer
	, p.numbercreditcards
	, p.networth
	, nullif(btrim(btrim(btrim(btrim(btrim(
	  case
		when p.networth > 1000000 or p.income > 200000
		then 'HighValue'
		else ''
	  end
	  || '+' ||
	  case
		when p.numberchildren > 3 or p.numbercreditcards > 5
		then 'Expenses'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.age > 45
		then 'Boomer'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.income < 50000 or p.creditrating < 600 or p.networth < 100000
		then 'MoneyAlert'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.numbercars > 3 or p.numbercreditcards > 7
		then 'Spender'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.age < 25 and p.networth > 1000000
		then 'Inherited'
		else ''
	  end
	  , '+'), '')
	from staging.prospect p
RETURNING *)

select count(*) into inserted_row_counts from inserted_rows;
