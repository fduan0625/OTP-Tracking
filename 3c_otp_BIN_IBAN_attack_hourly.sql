INSERT OVERWRITE TABLE fduan.otp_dy_pmts_hourly PARTITION(event_utc_date,event_utc_hour)
select pm.account_id
		,pm.country_code
		,pm.event_utc_ms_ts 
		,pm.processor_response
		,pm.tmx_policy_score
		,pm.card_type
		,pm.mop_type
		,pm.mop_institution as iban_bic
		,pm.cc_bin_number
		,pm.row_num
		,dy.member_status
		,dy.freetrial_t_f
		,dy.visitor_device_id
		,dy.device_esn
		,dy.country
		,dy.city
		,dy.triggering_mop_mode
		,dy.otp_trigger_utc_ms
		,dy.otp_trigger_dateint
		,dy.otp_phone_country_code
		,dy.phone_country_mismatch
		,dy.otp_success_signup
		,pm.event_utc_date
		,pm.event_utc_hour

from fduan.otp_pmt_event_hourly pm
	left join fduan.otp_dy_journey dy
	on pm.account_id = dy.in_acct_id
where event_utc_date between nf_dateadd(nf_dateint_today() , -1) and nf_dateint_today() 
;







