SET TEST_START_DATEINT = 20181115;
SET TEST_END_DATEINT = 20190115; 
SET SIGNUP_REV_WINDOW = 63; -- only consider paid P2 revenue

-- CREATE TABLE if not exists fduan.otp_phone_country_code(
--        in_acct_id        			BIGINT,
--        otp_phone_country_code       STRING,
--        carrier_type					STRING,
--        row_num                      INT

--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_phone_country_code PARTITION(dateint,hour)
		select cast(other_properties['transaction.input.account_owner_id'] as bigint) as in_acct_id
		,other_properties['transaction.otp_phone_country_code'] as otp_phone_country_code
		,other_properties['transaction.carrier_type'] as carrier_type
		,row_number() OVER (partition by other_properties['transaction.input.account_owner_id'] order by cast(other_properties['event_utc_ms'] as BIGINT) desc) as row_num
		,dateint
		,hour
		from default.dynecom_transaction_events
		where UPPER(other_properties['transaction.transaction_class']) like '%OTP%'
  		 and other_properties['transaction.otp_phone_country_code'] is not null
  		 and 
  		 (
  		 (dateint = nf_dateint_today() and hour = nf_hour(nf_timestamp_now())) OR
  		 (dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and hour = nf_hour(nf_timestamp_now() - interval '1' hour))
  		 )
		;



-- CREATE TABLE if not exists fduan.otp_phone_country_dedup(
--        in_acct_id        			BIGINT,
--        otp_phone_country_code       STRING,
--        carrier_type					STRING,
--        voip_t_f						INT

--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_phone_country_dedup PARTITION(dateint,hour)
		select base.in_acct_id
		,base.otp_phone_country_code
		,base.carrier_type
		,a.voip_t_f
		,base.dateint
		,base.hour
		from fduan.otp_phone_country_code base
		inner join (select in_acct_id
			,max(case when carrier_type='voip' then 1 else 0 end) as voip_t_f
			from fduan.otp_phone_country_code
			group by 1
			)a
		on base.in_acct_id = a.in_acct_id
		where base.row_num=1
		and   (
    			(dateint = nf_dateint_today() and hour = nf_hour(nf_timestamp_now())) OR
    			(dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and hour = nf_hour(nf_timestamp_now() - interval '1' hour))
    			)
		;
