SET TEST_START_DATEINT = 20181115;
SET TEST_END_DATEINT = 20190115; 
SET SIGNUP_REV_WINDOW = 63; -- only consider paid P2 revenue

-- CREATE TABLE if not exists fduan.otp_dy_trigger(
--        in_acct_id        	BIGINT,
--        member_status     	STRING,
--        freetrial_t_f     	INT,
--        plan_id           	STRING,
--        visitor_device_id 	STRING,
--        device_esn			STRING,
--        device_type			STRING,
--        device_group			STRING,
--        country				STRING,
--        city					STRING,
--        triggering_mop_raw	STRING,
--        otp_trigger_utc_ms	BIGINT,
--        otp_trigger_dateint	BIGINT,
--        row_num 				INT

--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_dy_trigger PARTITION(dateint,hour)
		select cast(other_properties['input.account_owner_id'] as bigint) as in_acct_id
		,other_properties['output.visitor_state'] as member_status
		,case when other_properties['output.is_free_trial'] = 'true' then 1 else 0 end as freetrial_t_f
		,other_properties['output.saved_plan_offer_id'] as plan_id
		,restricted_do_not_copy['output.visitor_device_id'] as visitor_device_id
    	,restricted_do_not_copy['device.esn'] as device_esn
		,other_properties['device.category'] as device_type
		,other_properties['device.category_group'] as device_group
		,other_properties['geo.country'] as country
		,other_properties['geo.city'] as city
		,other_properties['input.mode'] as triggering_mop_raw
		,cast(other_properties['event_utc_ms'] as BIGINT) as otp_trigger_utc_ms
		,dateint as otp_trigger_dateint
		,row_number() OVER (partition by other_properties['input.account_owner_id'] order by cast(other_properties['event_utc_ms'] as bigint) desc) as row_num
		,dateint
		,hour
		from default.dynecom_execution_events   
		where other_properties['output.mode']='enterOTPPhoneMode'  -- This indicates otp triggered yes or no
		and other_properties['input.mode'] != 'enterOTPPhoneMode'
		and 
  		 (
  		 (dateint = nf_dateint_today() and hour = nf_hour(nf_timestamp_now())) OR
  		 (dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and hour = nf_hour(nf_timestamp_now() - interval '1' hour))
  		 )
		;

-- CREATE TABLE if not exists fduan.otp_dy_trigger_dedup(
--        in_acct_id        	BIGINT,
--        member_status     	STRING,
--        freetrial_t_f     	INT,
--        plan_id           	STRING,
--        visitor_device_id 	STRING,
--        device_esn			STRING,
--        device_type			STRING,
--        device_group			STRING,
--        country				STRING,
--        city					STRING,
--        triggering_mop_raw	STRING,
--        otp_trigger_utc_ms	BIGINT,
--        otp_trigger_dateint	BIGINT,
--        otp_trigger_cnt		INT,
--        unique_device_cnt	INT

--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;

INSERT OVERWRITE TABLE fduan.otp_dy_trigger_dedup PARTITION(dateint,hour)
		select base.in_acct_id
			,base.member_status
			,base.freetrial_t_f
			,base.plan_id
			,base.visitor_device_id
			,base.device_esn
			,base.device_type
			,base.device_group
			,base.country
			,base.city
			,base.triggering_mop_raw
			,base.otp_trigger_utc_ms
			,base.otp_trigger_dateint
      		,a.otp_trigger_cnt
      		,a.unique_device_cnt
      		,base.dateint
      		,base.hour
		from fduan.otp_dy_trigger base
		inner join 
		(select in_acct_id
			,count(*) as otp_trigger_cnt
			,count(distinct device_esn) as unique_device_cnt
			from fduan.otp_dy_trigger
			group by 1) a
		on base.in_acct_id=a.in_acct_id
		where base.row_num=1
		and   (
    			(dateint = nf_dateint_today() and hour = nf_hour(nf_timestamp_now())) OR
    			(dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and hour = nf_hour(nf_timestamp_now() - interval '1' hour))
    			)
		;
