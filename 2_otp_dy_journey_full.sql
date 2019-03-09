SET TEST_START_DATEINT = 20181115;
SET TEST_END_DATEINT = 20190115; 
SET SIGNUP_REV_WINDOW = 63; -- only consider paid P2 revenue


-- CREATE TABLE if not exists fduan.otp_dy_journey(
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
--        triggering_mop_mode	STRING,
--        otp_trigger_utc_ms	BIGINT,
--        otp_trigger_dateint	BIGINT,
--        otp_trigger_cnt		INT,
--        unique_device_cnt	INT,
--        phone_entries		INT,
--        phone_provide_utc_ms 	BIGINT,
--        otp_phone_country_code   STRING,
--        phone_country_mismatch	INT,
--        carrier_type				STRING,
--        voip_t_f					INT,
--        code_page_utc_ms         BIGINT,
--        signup_utc_ms				BIGINT,
--        resendcode_t_f				INT,
--        switch_pmt_t_f				INT,
--        user_initiate_back_t_f		INT,
--        invalid_otp_t_f				INT,
--        retry_failure_t_f			INT,
--        pmt_failure_t_f				INT,
--        pmt_empty_t_f				INT,
--        phonenumber_empty_t_f		INT,
--        throttling_failure_t_f		INT,
--        otp_success_signup			INT,
--        otp_trigger_to_su_sec		BIGINT,
--        phone_provided_to_su_sec		BIGINT

--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_dy_journey PARTITION(dateint,hour)
select tr.in_acct_id
		,tr.member_status
		,tr.freetrial_t_f
		,tr.plan_id
		,tr.visitor_device_id
		,tr.device_esn
		,tr.device_type
		,tr.device_group
		,tr.country
		,tr.city
		,tr.triggering_mop_raw
		,case when tr.triggering_mop_raw in ("creditOptionMode","editcreditOptionMode") then 'credit'
			  when tr.triggering_mop_raw in ("deDebitOptionMode","editdeDebitOptionMode") then 'DE_DD'
			  when tr.triggering_mop_raw in ("debitOptionMode","editdebitOptionMode") then 'debit'
			  end as triggering_mop_mode

		,tr.otp_trigger_utc_ms
		,tr.otp_trigger_dateint
		,tr.otp_trigger_cnt
		,tr.unique_device_cnt
		,ph.phone_entries
		,ph.phone_provide_utc_ms
		,phc.otp_phone_country_code 
		,case when tr.country != phc.otp_phone_country_code then 1 else 0 end as phone_country_mismatch
		,phc.carrier_type 
		,phc.voip_t_f
		,co.code_page_utc_ms
		,co.signup_utc_ms
		,co.resendcode_t_f
		,co.switch_pmt_t_f
		,co.user_initiate_back_t_f
		,co.invalid_otp_t_f
		,co.retry_failure_t_f
		,co.pmt_failure_t_f
		,co.pmt_empty_t_f
		,co.phonenumber_empty_t_f
		,co.throttling_failure_t_f
		,co.otp_success_signup
		,(co.signup_utc_ms - tr.otp_trigger_utc_ms)/1000 as otp_trigger_to_su_sec
		,(co.signup_utc_ms - ph.phone_provide_utc_ms)/1000 as phone_provided_to_su_sec
		,tr.dateint
		,tr.hour
		from fduan.otp_dy_trigger_dedup tr
		-- inner join fduan.otp_cell_assignment ab
		-- 	on tr.in_acct_id = ab.account_id
		-- 	and ab.alloc_utc_ms<=tr.otp_trigger_utc_ms
		-- 	and ab.cell_nbr=2
		left join fduan.otp_phone_provided_dedup ph
			on tr.in_acct_id = ph.in_acct_id
			and (
    			(ph.dateint = nf_dateint_today() and ph.hour = nf_hour(nf_timestamp_now())) OR
    			(ph.dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and ph.hour = nf_hour(nf_timestamp_now() - interval '1' hour))
    			)
		left join fduan.otp_phone_country_dedup phc
			on tr.in_acct_id = phc.in_acct_id
			and (
    			(phc.dateint = nf_dateint_today() and phc.hour = nf_hour(nf_timestamp_now())) OR
    			(phc.dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and phc.hour = nf_hour(nf_timestamp_now() - interval '1' hour))
    			)
		left join fduan.otp_code_page_dedup co
			on tr.in_acct_id = co.in_acct_id
			and (
    			(co.dateint = nf_dateint_today() and co.hour = nf_hour(nf_timestamp_now())) OR
    			(co.dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and co.hour = nf_hour(nf_timestamp_now() - interval '1' hour))
    			)
		where 	(
    			(tr.dateint = nf_dateint_today() and tr.hour = nf_hour(nf_timestamp_now())) OR
    			(tr.dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and tr.hour = nf_hour(nf_timestamp_now() - interval '1' hour))
    			)
;


/** analysis for Tableau **/

select member_status
,freetrial_t_f
,device_group
,country
,case when triggering_mop_mode in ('credit','debit') then 'card'
	  when triggering_mop_mode = 'DE_DD' then 'DE_Direct_Debit'
	  end as triggering_mop_type
,nf_timestamp(otp_trigger_dateint) as otp_trigger_dateint
,otp_phone_country_code
,carrier_type
,voip_t_f
,count(*) as num_cust
,sum(case when phone_entries>=1 then 1 else 0 end) as num_provide_phone
,sum(case when phone_entries>=1 and phone_country_mismatch=1 then 1 else 0 end) as phone_country_mismatch
,sum(coalesce(resendcode_t_f,0)) as resendcode
,sum(coalesce(switch_pmt_t_f,0)) as switch_pmt
,sum(coalesce(invalid_otp_t_f,0)) as invalid_otp
,sum(coalesce(retry_failure_t_f,0)) as retry_failure
,sum(coalesce(pmt_failure_t_f,0)) as pmt_failure
,sum(coalesce(pmt_empty_t_f,0)) as pmt_empty
,sum(coalesce(phonenumber_empty_t_f,0)) as phonenumber_empty
,sum(coalesce(throttling_failure_t_f,0)) as throttling_failure
,sum(coalesce(otp_success_signup,0)) as otp_success_signup
,sum(case when otp_success_signup = 1 and freetrial_t_f = 0 then 1 else 0 end) as paid_signup
,sum(case when otp_success_signup = 1 and switch_pmt_t_f = 1 then 1 else 0 end) as switch_pmt_signup
,sum(case when otp_success_signup = 1 and phone_entries>=1 and phone_country_mismatch=1 then 1 else 0 end) as phone_cntry_mismatch_signup

,sum(case when otp_trigger_to_su_sec> 120 then 120
          when otp_trigger_to_su_sec is null then 0
          else otp_trigger_to_su_sec end) as otp_trigger_to_su_sec
,sum(case when phone_provided_to_su_sec > 120 then 120
			when phone_provided_to_su_sec is null then 0
			else phone_provided_to_su_sec end) as phone_provided_to_su_sec
from fduan.otp_dy_journey
where dateint>=20181115 --remove test accounts
group by 1,2,3,4,5,6,7,8,9;


/* checking if foreign numbers are travlers or fraudsters, and they are fraudsters */
select
a.triggering_mop_mode
,a.otp_phone_country_code as otp_phone_country
,b.country_iso_code as streaming_country
,count(*) as num_signup
from fduan.otp_dy_journey a
inner join (select account_id, country_iso_code, count(*) from fduan.otp_streaming_after_signup group by 1,2)b
on a.in_acct_id = b.account_id
where a.phone_country_mismatch=1
and a.country in ('DE','AU','US')
and a.otp_phone_country_code in ('DZ','IN','CO','MA')
group by 1,2,3;






