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
