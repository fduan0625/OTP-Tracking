INSERT OVERWRITE TABLE fduan.otp_cs_contact PARTITION (contact_date)

	select
	 fact_date
	, alloc_account_id
	, subscrn_id
	, allocation_region_date
	, ticket_gate_level0_desc
	, ticket_gate_level2_desc
	, signup_pmt_type_desc
	, cell_id
	, sum(makegood_cnt) makegood_cnt
	, sum(makegood_amt) makegood_amt
	, sum(answered_cnt) contact_cnt
	,max(fact_date) as contact_date
	from (
	select 
	c.*
	, alloc.*
	from dse.cs_contact_f c
	join dse.cs_contact_skill_d  skill 
	    on skill.contact_skill_id = c.contact_skill_id
	join fduan.otp_ablaze_metrics_pmt alloc
	    on c.account_id = alloc.alloc_account_id
	    and c.fact_date between alloc.allocation_region_date and nf_dateadd(alloc.allocation_region_date, 28)
	join dse.cs_transfer_type_d  t 
	    on c.transfer_type_id = t.transfer_type_id
	join dse.cs_contact_subchannel_d sub 
	    on c.contact_subchannel_id=sub.contact_subchannel_id
	
	where (c.fact_utc_date between nf_dateadd(nf_dateint_today() , -3) and nf_dateint_today()) 
		and (c.fact_date between nf_dateadd(nf_dateint_today() , -3) and nf_dateint_today()) 
	    and t.major_transfer_type_desc not in ('TRANSFER_OUT')
	    and sub.contact_channel_id in ('Phone','Chat')
	    and c.answered_cnt=1
	    and skill.escalation_code not in ('G-Escalation', 'SC-Consult','SC-Escalation')
	)x
	
	group by
	fact_date
	, alloc_account_id
	, subscrn_id
	, allocation_region_date
	, ticket_gate_level0_desc
	, ticket_gate_level2_desc
	, signup_pmt_type_desc
	, cell_id
	;

