SET TEST_START_DATEINT = 20181115;
SET TEST_END_DATEINT = 20190115; 
SET SIGNUP_REV_WINDOW = 63; -- only consider paid P2 revenue


   -- DROP TABLE IF EXISTS fduan.otp_pmt_event_hourly;

   -- CREATE TABLE if not exists fduan.otp_pmt_event_hourly(
       -- account_id		  	     BIGINT,
       -- country_code          	 STRING,
       -- event_utc_ms_ts       	 BIGINT,
       -- membership_status		 STRING,
       -- billing_period_nbr		 INT,
       -- response_code 			 BIGINT,
       -- processor_response 		 STRING,
       -- tmx_policy_score			 INT,
       -- card_type				 STRING,
       -- mop_type 				 STRING,
       -- mop_institution			 STRING,
   	   -- cc_bin_number			 BIGINT,
       -- row_num 					 INT

   -- ) 
   -- PARTITIONED by (event_utc_date INT,event_utc_hour INT) 
   -- STORED AS PARQUET;


   INSERT OVERWRITE TABLE fduan.otp_pmt_event_hourly PARTITION(event_utc_date,event_utc_hour)
	select  
	base.account_id
	,base.country_code
	,base.event_utc_ms_ts
	,base.degenerate_values_map['membership_status'] as membership_status
	,cast(base.degenerate_values_map['billing_period_nbr'] as int) as billing_period_nbr
	,cast(base.degenerate_values_map['pmt_processor_response_code'] as bigint) as response_code
	,case when base.degenerate_values_map['pmt_processor_response_code'] = '-10476' then 'OTP' else 'decline' end as processor_response
	,cast(base.degenerate_values_map['tmx_policy_score'] as INT) as tmx_policy_score
	,base.degenerate_values_map['bin_metadata_type'] as card_type ---CREDIT.  DEBIT. PREPAID
	,base.degenerate_values_map['mop_type'] as mop_type----EU_DIRECT_DEBIT CC
	,base.degenerate_values_map['mop_institution'] as mop_institution
	,cast(m.clear_bin as bigint) as cc_bin_number
	,row_number() OVER (partition by base.account_id order by base.event_utc_ms_ts desc) as row_num
	,base.event_utc_date
	,base.event_utc_hour
	  from dse.pmt_events_hourly_f base
	  left join dse.pmt_mop_metadata_d m
	  on m.pmt_mop_metadata_sk = base.dimension_sk_map['pmt_mop_metadata_sk']
	  and m.clear_bin != 'Unknown'
	where event_utc_date between nf_dateadd(nf_dateint_today() , -1) and nf_dateint_today() 
	and base.pmt_event_type = 'payment_transaction'
	and cast(base.degenerate_values_map['billing_period_nbr'] as int) <=1
	and ((degenerate_values_map['mop_type'] ='CC') 
	    or (degenerate_values_map['mop_type'] = 'EU_DIRECT_DEBIT' and country_code='DE'))
	and degenerate_values_map['tmx_policy_score'] != '--'
	and cast(degenerate_values_map['tmx_policy_score'] as INT)  >= -80
	and degenerate_values_map['membership_status'] !='CURRENT_MEMBER'
	and base.degenerate_values_map['pmt_processor_response_code'] in ('-10476','-10031')
	;


/*** Back fill for cc_bin_number ***/



   INSERT OVERWRITE TABLE fduan.otp_pmt_event_hourly PARTITION(event_utc_date,event_utc_hour)
	select  
	base.account_id
	,base.country_code
	,base.event_utc_ms_ts
	,base.degenerate_values_map['membership_status'] as membership_status
	,cast(base.degenerate_values_map['billing_period_nbr'] as int) as billing_period_nbr
	,cast(base.degenerate_values_map['pmt_processor_response_code'] as bigint) as response_code
	,case when base.degenerate_values_map['pmt_processor_response_code'] = '-10476' then 'OTP' else 'decline' end as processor_response
	,cast(base.degenerate_values_map['tmx_policy_score'] as INT) as tmx_policy_score
	,base.degenerate_values_map['bin_metadata_type'] as card_type ---CREDIT.  DEBIT. PREPAID
	,base.degenerate_values_map['mop_type'] as mop_type----EU_DIRECT_DEBIT CC
	,base.degenerate_values_map['mop_institution'] as mop_institution
	,cast(m.clear_bin as bigint) as cc_bin_number
	,row_number() OVER (partition by base.account_id order by base.event_utc_ms_ts desc) as row_num
	,base.event_utc_date
	,base.event_utc_hour
	  from dse.pmt_events_hourly_f base
	  left join dse.pmt_mop_metadata_d m
	  on m.pmt_mop_metadata_sk = base.dimension_sk_map['pmt_mop_metadata_sk']
	  and m.clear_bin != 'Unknown'
	where event_utc_date between 20190117 and 20190119
	and base.pmt_event_type = 'payment_transaction'
	and cast(base.degenerate_values_map['billing_period_nbr'] as int) <=1
	and ((degenerate_values_map['mop_type'] ='CC') 
	    or (degenerate_values_map['mop_type'] = 'EU_DIRECT_DEBIT' and country_code='DE'))
	and degenerate_values_map['tmx_policy_score'] != '--'
	and cast(degenerate_values_map['tmx_policy_score'] as INT)  >= -80
	and degenerate_values_map['membership_status'] !='CURRENT_MEMBER'
	and base.degenerate_values_map['pmt_processor_response_code'] in ('-10476','-10031')
	;


   INSERT OVERWRITE TABLE fduan.otp_pmt_event_hourly PARTITION(event_utc_date,event_utc_hour)
	select  
	base.account_id
	,base.country_code
	,base.event_utc_ms_ts
	,base.degenerate_values_map['membership_status'] as membership_status
	,cast(base.degenerate_values_map['billing_period_nbr'] as int) as billing_period_nbr
	,cast(base.degenerate_values_map['pmt_processor_response_code'] as bigint) as response_code
	,case when base.degenerate_values_map['pmt_processor_response_code'] = '-10476' then 'OTP' else 'decline' end as processor_response
	,cast(base.degenerate_values_map['tmx_policy_score'] as INT) as tmx_policy_score
	,base.degenerate_values_map['bin_metadata_type'] as card_type ---CREDIT.  DEBIT. PREPAID
	,base.degenerate_values_map['mop_type'] as mop_type----EU_DIRECT_DEBIT CC
	,base.degenerate_values_map['mop_institution'] as mop_institution
	,cast(m.clear_bin as bigint) as cc_bin_number
	,row_number() OVER (partition by base.account_id order by base.event_utc_ms_ts desc) as row_num
	,base.event_utc_date
	,base.event_utc_hour
	  from dse.pmt_events_hourly_f base
	  left join dse.pmt_mop_metadata_d m
	  on m.pmt_mop_metadata_sk = base.dimension_sk_map['pmt_mop_metadata_sk']
	  and m.clear_bin != 'Unknown'
	where event_utc_date between 20190120 and 20190122
	and base.pmt_event_type = 'payment_transaction'
	and cast(base.degenerate_values_map['billing_period_nbr'] as int) <=1
	and ((degenerate_values_map['mop_type'] ='CC') 
	    or (degenerate_values_map['mop_type'] = 'EU_DIRECT_DEBIT' and country_code='DE'))
	and degenerate_values_map['tmx_policy_score'] != '--'
	and cast(degenerate_values_map['tmx_policy_score'] as INT)  >= -80
	and degenerate_values_map['membership_status'] !='CURRENT_MEMBER'
	and base.degenerate_values_map['pmt_processor_response_code'] in ('-10476','-10031')
	;



   INSERT OVERWRITE TABLE fduan.otp_pmt_event_hourly PARTITION(event_utc_date,event_utc_hour)
	select  
	base.account_id
	,base.country_code
	,base.event_utc_ms_ts
	,base.degenerate_values_map['membership_status'] as membership_status
	,cast(base.degenerate_values_map['billing_period_nbr'] as int) as billing_period_nbr
	,cast(base.degenerate_values_map['pmt_processor_response_code'] as bigint) as response_code
	,case when base.degenerate_values_map['pmt_processor_response_code'] = '-10476' then 'OTP' else 'decline' end as processor_response
	,cast(base.degenerate_values_map['tmx_policy_score'] as INT) as tmx_policy_score
	,base.degenerate_values_map['bin_metadata_type'] as card_type ---CREDIT.  DEBIT. PREPAID
	,base.degenerate_values_map['mop_type'] as mop_type----EU_DIRECT_DEBIT CC
	,base.degenerate_values_map['mop_institution'] as mop_institution
	,cast(m.clear_bin as bigint) as cc_bin_number
	,row_number() OVER (partition by base.account_id order by base.event_utc_ms_ts desc) as row_num
	,base.event_utc_date
	,base.event_utc_hour
	  from dse.pmt_events_hourly_f base
	  left join dse.pmt_mop_metadata_d m
	  on m.pmt_mop_metadata_sk = base.dimension_sk_map['pmt_mop_metadata_sk']
	  and m.clear_bin != 'Unknown'
	where event_utc_date between 20190123 and 20190125
	and base.pmt_event_type = 'payment_transaction'
	and cast(base.degenerate_values_map['billing_period_nbr'] as int) <=1
	and ((degenerate_values_map['mop_type'] ='CC') 
	    or (degenerate_values_map['mop_type'] = 'EU_DIRECT_DEBIT' and country_code='DE'))
	and degenerate_values_map['tmx_policy_score'] != '--'
	and cast(degenerate_values_map['tmx_policy_score'] as INT)  >= -80
	and degenerate_values_map['membership_status'] !='CURRENT_MEMBER'
	and base.degenerate_values_map['pmt_processor_response_code'] in ('-10476','-10031')
	;



   INSERT OVERWRITE TABLE fduan.otp_pmt_event_hourly PARTITION(event_utc_date,event_utc_hour)
	select  
	base.account_id
	,base.country_code
	,base.event_utc_ms_ts
	,base.degenerate_values_map['membership_status'] as membership_status
	,cast(base.degenerate_values_map['billing_period_nbr'] as int) as billing_period_nbr
	,cast(base.degenerate_values_map['pmt_processor_response_code'] as bigint) as response_code
	,case when base.degenerate_values_map['pmt_processor_response_code'] = '-10476' then 'OTP' else 'decline' end as processor_response
	,cast(base.degenerate_values_map['tmx_policy_score'] as INT) as tmx_policy_score
	,base.degenerate_values_map['bin_metadata_type'] as card_type ---CREDIT.  DEBIT. PREPAID
	,base.degenerate_values_map['mop_type'] as mop_type----EU_DIRECT_DEBIT CC
	,base.degenerate_values_map['mop_institution'] as mop_institution
	,cast(m.clear_bin as bigint) as cc_bin_number
	,row_number() OVER (partition by base.account_id order by base.event_utc_ms_ts desc) as row_num
	,base.event_utc_date
	,base.event_utc_hour
	  from dse.pmt_events_hourly_f base
	  left join dse.pmt_mop_metadata_d m
	  on m.pmt_mop_metadata_sk = base.dimension_sk_map['pmt_mop_metadata_sk']
	  and m.clear_bin != 'Unknown'
	where event_utc_date between 20190126 and 20190128
	and base.pmt_event_type = 'payment_transaction'
	and cast(base.degenerate_values_map['billing_period_nbr'] as int) <=1
	and ((degenerate_values_map['mop_type'] ='CC') 
	    or (degenerate_values_map['mop_type'] = 'EU_DIRECT_DEBIT' and country_code='DE'))
	and degenerate_values_map['tmx_policy_score'] != '--'
	and cast(degenerate_values_map['tmx_policy_score'] as INT)  >= -80
	and degenerate_values_map['membership_status'] !='CURRENT_MEMBER'
	and base.degenerate_values_map['pmt_processor_response_code'] in ('-10476','-10031')
	;



   INSERT OVERWRITE TABLE fduan.otp_pmt_event_hourly PARTITION(event_utc_date,event_utc_hour)
	select  
	base.account_id
	,base.country_code
	,base.event_utc_ms_ts
	,base.degenerate_values_map['membership_status'] as membership_status
	,cast(base.degenerate_values_map['billing_period_nbr'] as int) as billing_period_nbr
	,cast(base.degenerate_values_map['pmt_processor_response_code'] as bigint) as response_code
	,case when base.degenerate_values_map['pmt_processor_response_code'] = '-10476' then 'OTP' else 'decline' end as processor_response
	,cast(base.degenerate_values_map['tmx_policy_score'] as INT) as tmx_policy_score
	,base.degenerate_values_map['bin_metadata_type'] as card_type ---CREDIT.  DEBIT. PREPAID
	,base.degenerate_values_map['mop_type'] as mop_type----EU_DIRECT_DEBIT CC
	,base.degenerate_values_map['mop_institution'] as mop_institution
	,cast(m.clear_bin as bigint) as cc_bin_number
	,row_number() OVER (partition by base.account_id order by base.event_utc_ms_ts desc) as row_num
	,base.event_utc_date
	,base.event_utc_hour
	  from dse.pmt_events_hourly_f base
	  left join dse.pmt_mop_metadata_d m
	  on m.pmt_mop_metadata_sk = base.dimension_sk_map['pmt_mop_metadata_sk']
	  and m.clear_bin != 'Unknown'
	where event_utc_date between 20190129 and 20190130
	and base.pmt_event_type = 'payment_transaction'
	and cast(base.degenerate_values_map['billing_period_nbr'] as int) <=1
	and ((degenerate_values_map['mop_type'] ='CC') 
	    or (degenerate_values_map['mop_type'] = 'EU_DIRECT_DEBIT' and country_code='DE'))
	and degenerate_values_map['tmx_policy_score'] != '--'
	and cast(degenerate_values_map['tmx_policy_score'] as INT)  >= -80
	and degenerate_values_map['membership_status'] !='CURRENT_MEMBER'
	and base.degenerate_values_map['pmt_processor_response_code'] in ('-10476','-10031')
	;

