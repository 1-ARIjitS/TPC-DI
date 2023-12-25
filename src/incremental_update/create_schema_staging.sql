drop table if exists staging.cashtransaction_b2;
create table staging.cashtransaction_b2 (
    cdc_flag char(1),
    cdc_dsn numeric(12) not null,
	ct_ca_id numeric(11) not null check(ct_ca_id >= 0),
	ct_dts timestamp not null,
	ct_amt numeric(10, 2) not null,
	ct_name char(100) not null
);

drop table if exists staging.dailymarket_b2;
create table staging.dailymarket_b2(
    cdc_flag char(1),
    cdc_dsn numeric(12) not null,
	dm_date date not null,
	dm_s_symb char(15) not null,
	dm_close numeric(8, 2) not null,
	dm_high numeric(8, 2) not null,
	dm_low numeric(8, 2) not null,
	dm_vol numeric(12) not null check(dm_vol >= 0)
);

drop table if exists staging.holdinghistory_b2;
create table staging.holdinghistory_b2(
    cdc_flag char(1),
    cdc_dsn numeric(12) not null,
	hh_h_t_id numeric(15) not null check(hh_h_t_id >= 0),
	hh_t_id numeric(15) not null check(hh_t_id >= 0),
	hh_before_qty numeric(6) not null check(hh_before_qty >= 0),
	hh_after_qty numeric(6) not null check(hh_after_qty >= 0)
);

drop table if exists staging.watchhistory_b2;
create table staging.watchhistory_b2(
    cdc_flag char(1),
    cdc_dsn numeric(12) not null,
	w_c_id numeric(11) not null check(w_c_id >= 0),
	w_s_symb char(15) not null,
	w_dts timestamp not null,
	w_action char(4) check(w_action in ('ACTV', 'CNCL'))
);

drop table if exists staging.trade_b2;
create table staging.trade_b2(
    cdc_flag char(1),
    cdc_dsn numeric(12) not null,
	t_id numeric(15) not null check(t_id >= 0),
	t_dts timestamp not null,
	t_st_id char(4) not null,
	t_tt_id char(3) not null,
	t_is_cash integer check(t_is_cash in (0, 1)),
	t_s_symb char(15) not null,
	t_qty numeric(6) check(t_qty >= 0),
	t_bid_price numeric(8,2) check(t_bid_price >= 0),
	t_ca_id numeric(11) not null check(t_ca_id >= 0),
	t_exec_name char(49) not null,
	t_trade_price numeric(8,2) check((t_st_id = 'CMPT' and t_trade_price >= 0) or (t_st_id != 'CMPT' and t_trade_price is null)),
	t_chrg numeric(10,2) check((t_st_id = 'CMPT' and t_chrg >= 0) or (t_st_id != 'CMPT' and t_chrg is null)),
	t_comm numeric(10,2) check((t_st_id = 'CMPT' and t_comm >= 0) or (t_st_id != 'CMPT' and t_comm is null)),
	t_tax numeric(10,2) check((t_st_id = 'CMPT' and t_tax >= 0) or (t_st_id != 'CMPT' and t_tax is null))
);

drop table if exists staging.customer;
create table staging.customer(
    cdc_flag char(1),
    cdc_dsn numeric(12) not null,
	c_id numeric(15) not null,
    c_tax_id char(20) not null,
    c_st_id char(4),
    c_l_name char(25) not null,
    c_f_name char(20) not null,
    c_m_name char(1),
    c_gndr char(1),
    c_tier numeric(1),
    c_dob date not null,
    c_adline1 char(80) not null,
    c_adline2 char(80),
    c_zipcode char(12) not null,
    c_city char(25) not null,
    c_state_prov char(20) not null,
    c_ctry char(24),
    c_ctry_1 char(3),
    c_area_1 char(3),
    c_local_1 char(10),
    c_ext_1 char(5),
    c_ctry_2 char(3),
    c_area_2 char(3),
    c_local_2 char(10),
    c_ext_2 char(5),
    c_ctry_3 char(3),
    c_area_3 char(3),
    c_local_3 char(10),
    c_ext_3 char(5),
    c_email_1 char(50),
    c_email_2 char(50),
    c_lcl_tx_id char(4) not null,
    c_nat_tx_id char(4) not null
);

drop table if exists staging.account;
create table staging.account(
    cdc_flag char(1),
    cdc_dsn numeric(12) not null,
    ca_id numeric(15) not null,
    ca_b_id numeric(15) not null,
    ca_c_id numeric(15) not null,
    ca_name char(50),
    ca_tax_st numeric(1),
    ca_st_id char(4)
);
