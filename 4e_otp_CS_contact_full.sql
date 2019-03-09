
   DROP TABLE IF EXISTS fduan.otp_cs_contact;

   CREATE TABLE if not exists fduan.otp_cs_contact(
   	   fact_date 						 	INT,
       alloc_account_id		  	     		BIGINT,
       subscrn_id 				 			BIGINT,
       allocation_region_date				INT,
       ticket_gate_level0_desc 				STRING,
       ticket_gate_level2_desc  			STRING,
       signup_pmt_type_desc 				STRING,
       cell_id				 			 	INT,
       makegood_cnt							INT,
       makegood_amt 						DOUBLE,
       contact_cnt 							INT

   ) 
   PARTITIONED by (contact_date INT) 
   STORED AS PARQUET;



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
	
	where (c.fact_utc_date >= 20181114 and c.fact_date >= 20181114) -- update later
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



/*** production code ***/

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
		 and (c.fact_date between nf_dateadd(nf_dateint_today() , -3) and nf_dateint_today()) -- update later
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






-- P2 renewal

-- with p2_renewals as (
-- select 
--  billing_original_period_start_date
-- , first_try_region_date
-- , last_try_region_date
-- , account_id
-- , subscrn_id
-- , txn_country
-- , nbs_order_id
-- , first_try_dimension_sk_map['pmt_mop_metadata_sk']ft_pmt_mop_metadata_sk
-- , first_try_dimension_sk_map['pmt_processor_response_sk']ft_pmt_processor_response_sk

-- , last_try_dimension_sk_map['pmt_mop_metadata_sk']lt_pmt_mop_metadata_sk
-- , last_try_dimension_sk_map['pmt_processor_response_sk']lt_pmt_processor_response_sk

-- , first_try_degenerate_values_map['txn_id']ft_txn_id
-- , first_try_degenerate_values_map['token_provider']ft_token_provider
-- , first_try_degenerate_values_map['crypto_used']ft_crypto_used
-- , first_try_degenerate_values_map['token_used']ft_token_used
-- , first_try_degenerate_values_map['process_by_debit_network']ft_process_by_debit_network
-- , first_try_degenerate_values_map['pmt_processor']ft_pmt_processor

-- , last_try_degenerate_values_map['txn_id']lt_txn_id
-- , last_try_degenerate_values_map['token_provider']lt_token_provider
-- , last_try_degenerate_values_map['crypto_used']lt_crypto_used
-- , last_try_degenerate_values_map['token_used']lt_token_used
-- , last_try_degenerate_values_map['process_by_debit_network']lt_process_by_debit_network
-- , last_try_degenerate_values_map['pmt_processor']lt_pmt_processor

-- , attempt_cnt_map['first_try_approved_cnt']first_try_approved_cnt
-- , attempt_cnt_map['chained_first_try_approved_cnt']chained_first_try_approved_cnt
-- , attempt_cnt_map['first_try_declined_cnt']first_try_declined_cnt
-- , attempt_cnt_map['chained_first_try_declined_cnt']chained_first_try_declined_cnt
-- , (attempt_cnt_map['chained_first_try_approved_cnt'] + attempt_cnt_map['first_try_approved_cnt']) first_try_postchain_cnt
 
-- , attempt_cnt_map['system_retry_approved_cnt']system_retry_approved_cnt
-- , attempt_cnt_map['chained_au_mop_update_approved_cnt'] + attempt_cnt_map['customer_mop_update_approved_cnt'] + attempt_cnt_map['chained_customer_mop_update_approved_cnt'] + attempt_cnt_map['au_mop_update_approved_cnt'] mop_update_approved_cnt
-- , attempt_cnt_map['system_retry_approved_cnt'] + attempt_cnt_map['chained_au_mop_update_approved_cnt'] + attempt_cnt_map['customer_mop_update_approved_cnt'] + attempt_cnt_map['chained_customer_mop_update_approved_cnt'] + attempt_cnt_map['au_mop_update_approved_cnt'] recovery_approved_cnt
-- , attempt_cnt_map['approved_cnt']approved_cnt
-- , attempt_cnt_map['first_try_attempt_cnt']first_try_attempt_cnt

-- from dse.pmt_renewal_acct_sum
-- where  billing_original_period_start_date >= 20180601 and  first_try_region_date >= 20180711
-- and billing_period_nbr = 2
-- )

-- select 
--  allocation_region_date
-- , first_try_region_date
-- , cell_id
-- , alloc_group_id
-- , t.country_iso_code
-- , geo.country_desc
-- , geo.estaff_subregion_desc
-- , adj_alloc_cnt
-- , signup_grouped_pmt_institution_desc
-- , signup_pmt_method_desc
-- , signup_pmt_type_desc
-- , signup_brand_desc
-- , billing_partner_desc
-- , clear_bin_6 signup_bin
-- , lmop.clear_bin as last_try_bin
-- , lmop.pmt_type_desc as last_try_pmt_type_desc
-- , lmop.affiliation as last_try_brand
-- , linst.pmt_institution_desc
-- , signup_issuer_country_code
-- , p1_hold_event_type
-- , token_provider signup_token_provider
-- , fraud_flag
-- , is_rejoin
-- , coalesce(ft_token_provider, '--')ft_token_provider
-- , coalesce(ft_crypto_used, 'false')ft_crypto_used
-- , coalesce(ft_token_used, 'false')ft_token_used
-- , coalesce(ft_process_by_debit_network, 'false')ft_process_by_debit_network
-- , ft_pmt_processor
-- , coalesce(frg.standardized_resp_desc, fresp.processor_response_desc, 'UNKNOWN')standardized_resp_desc
-- , fresp.processor_response_code
-- , fresp.processor_response_desc
-- , coalesce(lt_token_provider, '--')lt_token_provider
-- , coalesce(lt_crypto_used, 'false')lt_crypto_used
-- , coalesce(lt_token_used, 'false')lt_token_used
-- , coalesce(lt_process_by_debit_network, 'false')lt_process_by_debit_network
-- , lt_pmt_processor
-- , sum(first_try_approved_cnt) as first_try_approved_cnt
-- , sum(chained_first_try_approved_cnt) as chained_first_try_approved_cnt
-- , sum(first_try_postchain_cnt) as first_try_postchain_cnt
-- , sum(system_retry_approved_cnt) as system_retry_approved_cnt
-- , sum(mop_update_approved_cnt) as mop_update_approved_cnt
-- , sum(recovery_approved_cnt) as recovery_approved_cnt
-- , sum(approved_cnt) as approved_cnt
-- , sum(first_try_attempt_cnt) as first_try_attempt_cnt

-- from jguenther.test_10115_table t
-- join p2_renewals r
--     on t.alloc_account_id = r.account_id
--     and t.subscrn_id = r.subscrn_id
-- join dse.pmt_processor_response_v2_d fresp
--     on r.ft_pmt_processor_response_sk = fresp.pmt_processor_response_sk
-- left outer join dse.pmt_processor_response_grp_d frg
--     on r.ft_pmt_processor = frg.processor
--     and fresp.processor_response_code = frg.processor_response_code
-- join dse.geo_country_d geo
--     on t.country_iso_code = geo.country_iso_code
-- join dse.pmt_mop_metadata_d lmop
--     on lmop.pmt_mop_metadata_sk = r.lt_pmt_mop_metadata_sk
-- join dse.pmt_institution_v2_d linst
--     on linst.pmt_institution_sk = lmop.pmt_institution_sk
-- where t.adj_alloc_cnt = 1
-- group by
--  allocation_region_date
--  , first_try_region_date
-- , cell_id
-- , alloc_group_id
-- , t.country_iso_code
-- , geo.country_desc
-- , geo.estaff_subregion_desc
-- , adj_alloc_cnt
-- , signup_grouped_pmt_institution_desc
-- , signup_pmt_method_desc
-- , signup_pmt_type_desc
-- , signup_brand_desc
-- , billing_partner_desc
-- , clear_bin_6 
-- , lmop.clear_bin 
-- , lmop.pmt_type_desc
-- , lmop.affiliation 
-- , linst.pmt_institution_desc
-- , signup_issuer_country_code
-- , p1_hold_event_type
-- , token_provider
-- , fraud_flag
-- , is_rejoin
-- , coalesce(ft_token_provider, '--')
-- , coalesce(ft_crypto_used, 'false')
-- , coalesce(ft_token_used, 'false')
-- , coalesce(ft_process_by_debit_network, 'false')
-- , ft_pmt_processor
-- , coalesce(frg.standardized_resp_desc, fresp.processor_response_desc, 'UNKNOWN')
-- , fresp.processor_response_code
-- , fresp.processor_response_desc
-- , coalesce(lt_token_provider, '--')
-- , coalesce(lt_crypto_used, 'false')
-- , coalesce(lt_token_used, 'false')
-- , coalesce(lt_process_by_debit_network, 'false')
-- , lt_pmt_processor
-- ;


create table fduan.otp_cs_contact_with_ts as
	(select 
		base.*,fd.is_signup, fd.signup_utc_ts_ms,cc.min_contact_start_utc_ts, cc.max_contact_start_utc_ts
		from fduan.otp_cs_contact base
		left join (select alloc_account_id,is_signup, min(signup_utc_ts_ms) as signup_utc_ts_ms
					from fduan.otp_ablaze_metrics_pmt
					group by 1,2) fd
		on base.alloc_account_id = fd.alloc_account_id

		left join (select account_id
			, min(contact_start_utc_ts) as min_contact_start_utc_ts
			, max(contact_start_utc_ts) as max_contact_start_utc_ts
			from dse.cs_contact_f
			where fact_utc_date between 20181114 and 20181217
			group by 1) cc
		on base.alloc_account_id = cc.account_id
		)



select is_signup,cell_id, sum(contact_cnt)  as num_contacts
from fduan.otp_cs_contact_with_ts
where contact_date= allocation_region_date --- same day contact
and allocation_region_date <= 20181215 -- reduce to first test period
and (signup_utc_ts_ms is null --not signup
OR nf_to_unixtime_ms(min_contact_start_utc_ts)<= signup_utc_ts_ms --contact before signup
)
group by 1,2;




drop table fduan.otp_cs_contact_whole_population_with_ts;
create table fduan.otp_cs_contact_whole_population_with_ts  as
(
	select
	 fact_date
	, alloc_account_id
	, subscrn_id
	, allocation_region_date
	, ticket_gate_level0_desc
	, ticket_gate_level2_desc
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
	join dse.ab_nm_alloc_f alloc
	    on c.account_id = alloc.alloc_account_id
	    and c.fact_date between alloc.allocation_region_date and nf_dateadd(alloc.allocation_region_date, 28)
	    and test_id = 11440
		and
		country_iso_code <> 'DE'

		and
		coalesce(is_fraud, -1) in (0, -1)
		and
		coalesce(is_bot, -1) in (0, -1)
		and
		membership_status != 2
		and allocation_region_date between 20181114 and 20181215
	join dse.cs_transfer_type_d  t 
	    on c.transfer_type_id = t.transfer_type_id
	join dse.cs_contact_subchannel_d sub 
	    on c.contact_subchannel_id=sub.contact_subchannel_id
	
	where (c.fact_utc_date between 20181114 and 20181215) 
		 and (c.fact_date between 20181114 and 20181215) -- update later
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
	, cell_id
	

)

