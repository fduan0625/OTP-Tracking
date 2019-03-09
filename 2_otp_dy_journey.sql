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

