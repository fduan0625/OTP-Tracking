SET TEST_START_DATEINT = 20181115;
SET TEST_END_DATEINT = 20190115; 
SET SIGNUP_REV_WINDOW = 63; -- only consider paid P2 revenue

-- CREATE TABLE if not exists fduan.otp_phone_provided(
--        in_acct_id        		BIGINT,
--        phone_provide_utc_ms     BIGINT
--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;

INSERT OVERWRITE TABLE fduan.otp_phone_provided PARTITION(dateint,hour)
		select cast(other_properties['input.account_owner_id'] as bigint) as in_acct_id
		,cast(other_properties['event_utc_ms'] as bigint) as phone_provide_utc_ms
		,dateint
		,hour
		from default.dynecom_execution_events
		where other_properties['input.mode']='enterOTPPhoneMode' 
		and other_properties['output.mode']='enterOTPCodeMode'   -----input otpphone, output otpCode, indicates phone number provided
		and 
  		 (
  		 (dateint = nf_dateint_today() and hour = nf_hour(nf_timestamp_now())) OR
  		 (dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and hour = nf_hour(nf_timestamp_now() - interval '1' hour))
  		 )
;

-- CREATE TABLE if not exists fduan.otp_phone_provided_dedup(
--        in_acct_id        		BIGINT,
--        phone_entries			INT,
--        phone_provide_utc_ms     BIGINT
--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_phone_provided_dedup PARTITION(dateint,hour)
		select in_acct_id
		,count(*) as phone_entries
		,max(phone_provide_utc_ms) as phone_provide_utc_ms
		,max(dateint) as dateint
		,max(hour) as hour
		from fduan.otp_phone_provided
   		where (
    		(dateint = nf_dateint_today() and hour = nf_hour(nf_timestamp_now())) OR
    		(dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and hour = nf_hour(nf_timestamp_now() - interval '1' hour))
    		)
		group by 1
		;