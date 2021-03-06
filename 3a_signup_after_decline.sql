INSERT OVERWRITE TABLE fduan.otp_signup_after_decline PARTITION(signup_utc_date)
select base.account_id
	,ab.cell_nbr
	,a.country_code as pmt_decl_country_code
	,a.event_utc_ms_ts as pmt_decl_utc_ms_ts
	,a.event_utc_date as pmt_decl_utc_date
	,a.membership_status
	,a.billing_period_nbr
	,a.response_code
	,a.processor_response
	,a.tmx_policy_score
	,a.card_type
	,a.mop_type
	,case when a.card_type is not null then a.card_type 
	      when a.mop_type = 'EU_DIRECT_DEBIT' then 'EU_DIRECT_DEBIT'
	      end as decl_pmt_mop_type
	,base.subscrn_id
	,base.signup_utc_ts
	,nf_to_unixtime_ms(base.signup_utc_ts) signup_utc_ms
	,base.signup_date
	,base.country_iso_code
	,base.is_voluntary_cancel
	,base.latest_cancel_request_date
	,base.is_customer_initiated_cancel
	,base.is_free_trial_at_signup
	,cast(base.signup_plan_id as int) as signup_plan_id
	,cast(base.latest_plan_id as int) as latest_plan_id
	,base.mop_id  --Lookup table:  account_mop_d based on account_id, country_iso_code, mop_id
	,base.is_channel_giftcard
	,base.is_channel_ppp
	,base.signup_billing_partner_desc
	,base.is_dcb_signup
	,base.pmt_type_desc as signup_pmt_type
	,ROW_NUMBER() OVER (partition by base.account_id, base.subscrn_id order by signup_utc_ts desc) as row_num
	,nf_dateint(base.signup_utc_ts) signup_utc_date
	from fduan.otp_cell_assignment ab
	inner join fduan.otp_pmt_event_hourly a
		on ab.account_id = a.account_id
		and ab.alloc_utc_ms<=a.event_utc_ms_ts
		and ab.is_forced is null
	inner join dse.subscrn_d base
		on base.account_id = ab.account_id
		and nf_to_unixtime_ms(base.signup_utc_ts)>=a.event_utc_ms_ts
	where base.signup_date between nf_dateadd(nf_dateint_today() , -3) and nf_dateint_today() 
	and a.row_num=1
	;