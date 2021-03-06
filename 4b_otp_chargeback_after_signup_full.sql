
-- fduan.otp_chargebacks_after_signup  depend on 
-- 3a_signup_after_decline.sql script
-- dse.pmt_events_hourly_f, refreshed hourly
-- fduan.otp_signup_after_decline refreshed daily
-- schedule in the daily refresh job



DROP TABLE fduan.otp_chargebacks_after_signup_full_alloc;

CREATE TABLE if not exists fduan.otp_chargebacks_after_signup_full_alloc(
		account_id 					BIGINT,
		cell_nbr 					INT,
		country_code  				STRING,
		mop_id 						BIGINT,
		event_utc_ms_ts 			BIGINT,
		pmt_mop_metadata_sk 		STRING,
		billing_period_nbr 			INT,
		mop_type 					STRING,
		chargeback_notify_type 		STRING,
		membership_status 			STRING,
		chargeback_status 			STRING,
		chargeback_request_reason 	STRING,
		dispute_date_ts 			TIMESTAMP,
		bin_metadata_id 			STRING


   ) 
PARTITIONED by (event_utc_date INT, event_utc_hour INT) 
STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_chargebacks_after_signup_full_alloc PARTITION (event_utc_date,event_utc_hour)
	select  base.account_id
			,su.cell_id as cell_nbr
			,base.country_code
			,base.mop_id
			,base.event_utc_ms_ts
			,base.dimension_sk_map['pmt_mop_metadata_sk'] as pmt_mop_metadata_sk
			,cast(base.degenerate_values_map['billing_period'] as int) as billing_period_nbr
			,base.degenerate_values_map['mop_type'] as mop_type
			,base.degenerate_values_map['chargeback_notify_type'] as chargeback_notify_type
			,base.degenerate_values_map['membership_status'] as membership_status
			,base.degenerate_values_map['chargeback_status'] as chargeback_status
			,base.degenerate_values_map['chargeback_request_reason'] as chargeback_request_reason
			,base.degenerate_values_map['dispute_date_ts'] as dispute_date_ts
			,base.degenerate_values_map['bin_metadata_id'] as bin_metadata_id
			,base.event_utc_date
			,base.event_utc_hour

	from dse.pmt_events_hourly_f base
		inner join 
			(select account_id,cell_id,max(nf_dateint(signup_utc_ts_ms)) as signup_utc_date 
			from dse.ab_nm_alloc_f where test_id=11440 and signup_utc_ts_ms is not null
			group by 1,2) su
		on base.account_id = su.account_id
		and base.event_utc_date >= su.signup_utc_date
	where base.pmt_event_type = 'chargeback_transaction'
	and cast(base.degenerate_values_map['billing_period']as int)<=3
	and lower(base.degenerate_values_map['is_duplicate']) != 'true'
	and base.event_utc_date>=20181114
   ;

--- production code
INSERT OVERWRITE TABLE fduan.otp_chargebacks_after_signup_full_alloc PARTITION (event_utc_date,event_utc_hour)
	select  base.account_id
			,su.cell_id as cell_nbr
			,base.country_code
			,base.mop_id
			,base.event_utc_ms_ts
			,base.dimension_sk_map['pmt_mop_metadata_sk'] as pmt_mop_metadata_sk
			,cast(base.degenerate_values_map['billing_period'] as int) as billing_period_nbr
			,base.degenerate_values_map['mop_type'] as mop_type
			,base.degenerate_values_map['chargeback_notify_type'] as chargeback_notify_type
			,base.degenerate_values_map['membership_status'] as membership_status
			,base.degenerate_values_map['chargeback_status'] as chargeback_status
			,base.degenerate_values_map['chargeback_request_reason'] as chargeback_request_reason
			,base.degenerate_values_map['dispute_date_ts'] as dispute_date_ts
			,base.degenerate_values_map['bin_metadata_id'] as bin_metadata_id
			,base.event_utc_date
			,base.event_utc_hour

	from dse.pmt_events_hourly_f base
		inner join 
			(select account_id,cell_id,nf_dateint(max(signup_utc_ts_ms) as signup_utc_date 
			from dse.ab_nm_alloc_f where test_id=11440 and signup_utc_ts_ms is not null
			group by 1,2) su
		on base.account_id = su.account_id
		and base.event_utc_date >= su.signup_utc_date
	where base.pmt_event_type = 'chargeback_transaction'
	and cast(base.degenerate_values_map['billing_period']as int)<=3
	and lower(base.degenerate_values_map['is_duplicate']) != 'true'
	and base.event_utc_date>=20181114
   ;


/********** Chargeback for OTP decline only ***********/

DROP TABLE fduan.otp_chargebacks_after_signup

CREATE TABLE if not exists fduan.otp_chargebacks_after_signup(
		account_id 					BIGINT,
		cell_nbr 					INT,
		country_code  				STRING,
		mop_id 						BIGINT,
		event_utc_ms_ts 			BIGINT,
		pmt_mop_metadata_sk 		STRING,
		billing_period_nbr 			INT,
		mop_type 					STRING,
		chargeback_notify_type 		STRING,
		membership_status 			STRING,
		chargeback_status 			STRING,
		chargeback_request_reason 	STRING,
		dispute_date_ts 			TIMESTAMP,
		bin_metadata_id 			STRING


   ) 
PARTITIONED by (event_utc_date INT, event_utc_hour INT) 
STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_chargebacks_after_signup PARTITION (event_utc_date,event_utc_hour)
	select  base.account_id
			,su.cell_nbr
			,base.country_code
			,base.mop_id
			,base.event_utc_ms_ts
			,base.dimension_sk_map['pmt_mop_metadata_sk'] as pmt_mop_metadata_sk
			,cast(base.degenerate_values_map['billing_period'] as int) as billing_period_nbr
			,base.degenerate_values_map['mop_type'] as mop_type
			,base.degenerate_values_map['chargeback_notify_type'] as chargeback_notify_type
			,base.degenerate_values_map['membership_status'] as membership_status
			,base.degenerate_values_map['chargeback_status'] as chargeback_status
			,base.degenerate_values_map['chargeback_request_reason'] as chargeback_request_reason
			,base.degenerate_values_map['dispute_date_ts'] as dispute_date_ts
			,base.degenerate_values_map['bin_metadata_id'] as bin_metadata_id
			,base.event_utc_date
			,base.event_utc_hour

	from dse.pmt_events_hourly_f base
		inner join 
			(select account_id,cell_nbr,max(signup_utc_date) as signup_utc_date 
			from fduan.otp_signup_after_decline
			group by 1,2) su
		on base.account_id = su.account_id
		and base.event_utc_date >= su.signup_utc_date
	where base.pmt_event_type = 'chargeback_transaction'
	and cast(base.degenerate_values_map['billing_period']as int)<=3
	and lower(base.degenerate_values_map['is_duplicate']) != 'true'
	and base.event_utc_date>=20181114
   ;

--- production code
INSERT OVERWRITE TABLE fduan.otp_chargebacks_after_signup PARTITION (event_utc_date,event_utc_hour)
	select  base.account_id
			,su.cell_nbr
			,base.country_code
			,base.mop_id
			,base.event_utc_ms_ts
			,base.dimension_sk_map['pmt_mop_metadata_sk'] as pmt_mop_metadata_sk
			,cast(base.degenerate_values_map['billing_period'] as int) as billing_period_nbr
			,base.degenerate_values_map['mop_type'] as mop_type
			,base.degenerate_values_map['chargeback_notify_type'] as chargeback_notify_type
			,base.degenerate_values_map['membership_status'] as membership_status
			,base.degenerate_values_map['chargeback_status'] as chargeback_status
			,base.degenerate_values_map['chargeback_request_reason'] as chargeback_request_reason
			,base.degenerate_values_map['dispute_date_ts'] as dispute_date_ts
			,base.degenerate_values_map['bin_metadata_id'] as bin_metadata_id
			,base.event_utc_date
			,base.event_utc_hour

	from dse.pmt_events_hourly_f base
		inner join 
			(select account_id,cell_nbr,max(signup_utc_date) as signup_utc_date 
			from fduan.otp_signup_after_decline
			group by 1,2) su
		on base.account_id = su.account_id
		and base.event_utc_date >= su.signup_utc_date
	where base.pmt_event_type = 'chargeback_transaction'
	and cast(base.degenerate_values_map['billing_period']as int)<=3
	and lower(base.degenerate_values_map['is_duplicate']) != 'true'
	and base.event_utc_date between nf_dateadd(nf_dateint_today() , -3) and nf_dateint_today() 
   ;




 