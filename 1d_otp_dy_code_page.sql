SET TEST_START_DATEINT = 20181115;
SET TEST_END_DATEINT = 20190115; 
SET SIGNUP_REV_WINDOW = 63; -- only consider paid P2 revenue

-- CREATE TABLE if not exists fduan.otp_code_page(
--        in_acct_id        			BIGINT,
--        otp_code_page_utc_ms         BIGINT,
--        signup_utc_ms				BIGINT,
--        resendcode_t_f				INT,
--        switch_pmt_t_f				INT,
--        user_initiate_back_t_f		INT,
--        error_code					STRING,
--        invalid_otp_t_f				INT,
--        retry_failure_t_f			INT,
--        pmt_failure_t_f				INT,
--        pmt_empty_t_f				INT,
--        phonenumber_empty_t_f		INT,
--        throttling_failure_t_f		INT,
--        otp_success_signup			INT,
--        row_num                      INT

--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;



INSERT OVERWRITE TABLE fduan.otp_code_page PARTITION(dateint,hour)
		select 
		cast(other_properties['input.account_owner_id'] as bigint) as in_acct_id
		,cast(other_properties['event_utc_ms'] as bigint) as otp_code_page_utc_ms
		,(case when other_properties['output.visitor_state']='CURRENT_MEMBER' and other_properties['output.error_code'] is null then cast(other_properties['event_utc_ms'] as bigint) else null end) as signup_utc_ms

		,(case when other_properties['input.action']='resendCodeAction' then 1 else 0 end) as resendcode_t_f
		,(case when other_properties['output.mode']='payAndStartMembershipWithContext' then 1 else 0 end) as switch_pmt_t_f
		,(case when other_properties['input.action']='backAction' and other_properties['output.mode']!='payAndStartMembershipWithContext' then 1 else 0 end) as user_initiate_back_t_f
		,other_properties['output.error_code'] as error_code
		,(case when other_properties['output.error_code'] = 'otp_code_invalid' then 1 else 0 end) as invalid_otp_t_f
		,(case when other_properties['output.error_code'] = 'generic_retryable_failure' then 1 else 0 end) as retry_failure_t_f
		,(case when other_properties['output.error_code'] = 'generic_payment_failure' then 1 else 0 end) as pmt_failure_t_f
		,(case when other_properties['output.error_code'] = 'paymentChoice_is_empty' then 1 else 0 end) as pmt_empty_t_f
		,(case when other_properties['output.error_code'] = 'phoneNumber_is_empty' then 1 else 0 end) as phonenumber_empty_t_f
		,(case when other_properties['output.error_code'] = 'throttling_failure' then 1 else 0 end) as throttling_failure_t_f

		,(case when other_properties['output.visitor_state']='CURRENT_MEMBER' and other_properties['output.error_code'] is null then 1 else 0 end) as otp_success_signup
		,row_number() OVER (partition by other_properties['transaction.input.account_owner_id'] order by cast(other_properties['event_utc_ms'] as BIGINT) desc) as row_num
		,dateint
		,hour
		from default.dynecom_execution_events
		where other_properties['input.mode'] = 'enterOTPCodeMode'  -- This indicates user land or re-land on the code entry page after provide phone number
		and 
  		 (
  		 (dateint = nf_dateint_today() and hour = nf_hour(nf_timestamp_now())) OR
  		 (dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and hour = nf_hour(nf_timestamp_now() - interval '1' hour))
  		 )
  		 ;


-- CREATE TABLE if not exists fduan.otp_code_page_dedup(
--        in_acct_id        			BIGINT,
--        code_page_utc_ms        		BIGINT,
--        signup_utc_ms					BIGINT,
--        resendcode_t_f				INT,
--        switch_pmt_t_f				INT,
--        user_initiate_back_t_f		INT,
--        invalid_otp_t_f				INT,
--        retry_failure_t_f			INT,
--        pmt_failure_t_f				INT,
--        pmt_empty_t_f				INT,
--        phonenumber_empty_t_f		INT,
--        throttling_failure_t_f		INT,
--        otp_success_signup			INT

--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_code_page_dedup PARTITION(dateint,hour)
		select in_acct_id
		,max(otp_code_page_utc_ms) as code_page_utc_ms
		,max(signup_utc_ms) as signup_utc_ms
		,max(resendcode_t_f) as resendcode_t_f
		,max(switch_pmt_t_f) as switch_pmt_t_f
		,max(user_initiate_back_t_f) as user_initiate_back_t_f
		,max(invalid_otp_t_f) as invalid_otp_t_f
		,max(retry_failure_t_f) as retry_failure_t_f
		,max(pmt_failure_t_f) as pmt_failure_t_f
		,max(pmt_empty_t_f) as pmt_empty_t_f
		,max(phonenumber_empty_t_f) as phonenumber_empty_t_f
		,max(throttling_failure_t_f) as throttling_failure_t_f
		,max(otp_success_signup) as otp_success_signup
		,max(dateint) as dateint
		,max(hour) as hour
		from fduan.otp_code_page
		where 
			   (
    			(dateint = nf_dateint_today() and hour = nf_hour(nf_timestamp_now())) OR
    			(dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and hour = nf_hour(nf_timestamp_now() - interval '1' hour))
    			)
		group by 1;
		;

