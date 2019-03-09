INSERT OVERWRITE TABLE fduan.otp_final_dup PARTITION (allocation_region_date)
select 
      alloc.alloc_account_id
      ,alloc.allocation_utc_date
      ,alloc.subscrn_id
      ,alloc.country_iso_code
      ,alloc.cell_id
      ,alloc.test_id
      ,alloc.alloc_group_id
      ,alloc.is_fraud
      ,alloc.is_bot
      ,alloc.device_category
      ,alloc.allocation_utc_ts_ms
      ,alloc.signup_utc_ts_ms
      ,alloc.mop_utc_ts_ms
      ,alloc.is_signup
      ,alloc.is_mop_provided
      ,alloc.secs_to_signup
      ,alloc.is_free_trial_at_signup
      ,alloc.displayWidthPixel
      ,alloc.displayHeightPixel
      ,alloc.browserName
      ,alloc.signup_plan_rollup_id
      ,alloc.p2_is_vol_cancel
      ,alloc.p2_is_invol_cancel
      ,alloc.p2_gross_realized_revenue
      ,alloc.p2_net_realized_revenue
      ,alloc.max_period_paid_63d
      ,alloc.p2_earliest_plan_change_date
      ,alloc.p2_current_plan_rollup_id
      ,alloc.p2_streaming_secs
      ,alloc.p3_is_vol_cancel
      ,alloc.p3_is_invol_cancel
      ,alloc.p3_gross_realized_revenue
      ,alloc.p3_net_realized_revenue
      ,alloc.max_period_paid_98d
      ,alloc.p3_earliest_plan_change_date
      ,alloc.p3_current_plan_rollup_id
      ,alloc.p3_streaming_secs
      ,alloc.alloc_cnt                          
      ,alloc.decl_utc_ms_ts                       
      ,alloc.membership_status                   
      ,alloc.processor_response                  
      ,alloc.tmx_policy_score                     
      ,alloc.mop_type                            
      ,alloc.mop_institution                    
      ,alloc.cc_bin_number                      
             
      ,alloc.signup_pmt_type_desc
      ,alloc.signup_pmt_brand_desc
      ,alloc.signup_issuer_desc_grp
      ,alloc.signup_clear_bin
      ,alloc.signup_iban_bin
      ,alloc.p2_pmt_type_desc
      ,alloc.p2_pmt_brand_desc
      ,alloc.p2_issuer_desc_grp
      ,alloc.p2_clear_bin
      ,alloc.p2_iban_bin
      ,alloc.is_rejoin
      ,alloc.subscrn_d_fraud_flag

      ,otp.phone_country_mismatch
      ,otp.resendcode_t_f
      ,otp.switch_pmt_t_f
      ,otp.user_initiate_back_t_f
      ,otp.invalid_otp_t_f
      ,otp.retry_failure_t_f
      ,otp.pmt_failure_t_f
      ,otp.pmt_empty_t_f
      ,otp.phonenumber_empty_t_f
      ,otp.throttling_failure_t_f
      ,otp.otp_success_signup
      ,otp.otp_trigger_to_su_sec

      ,pay.p1_vol_cancel_cnt
      ,pay.p1_invol_cancel_cnt
      ,pay.p1_onhold_cnt
      ,pay.p1_complete_cnt
      ,pay.p2_vol_cancel_cnt
      ,pay.p2_invol_cancel_cnt
      ,pay.p2_onhold_cnt
      ,pay.p2_complete_cnt
      ,pay.fraud_flag
      ,pay.paid_t_f

      ,case when cb.account_id is not null then 1 else 0 end as chargeback_t_f

      ,pbf.xborder_streaming_t_f
      ,pbf.LATAM_stream_t_f
      ,pbf.LATAM_signup_t_f
      ,pbf.num_profiles
      ,pbf.num_unique_titles
      ,pbf.total_view_min
      ,pbf.total_qualified_view_min

      ,rev.average_paid_days_63d
      ,rev.average_paid_days_98d
      ,rev.net_realized_revenue_63d
      ,rev.net_realized_revenue_98d
      ,rev.gross_realized_revenue_63d
      ,rev.gross_realized_revenue_98d

      ,case when cs.alloc_account_id is not null then 1 else 0 end as ever_contact_cs_t_f
      ,cs.total_cs_call

      ,alloc.allocation_region_date

from fduan.otp_ablaze_metrics_pmt alloc 
left join (select in_acct_id
               , max(phone_country_mismatch) as phone_country_mismatch
               , max(resendcode_t_f) as resendcode_t_f
               , max(switch_pmt_t_f) as switch_pmt_t_f
               , max(user_initiate_back_t_f) as user_initiate_back_t_f
               , max(invalid_otp_t_f) as invalid_otp_t_f
               , max(retry_failure_t_f) as retry_failure_t_f
               , max(pmt_failure_t_f) as pmt_failure_t_f
               , max(pmt_empty_t_f) as pmt_empty_t_f
               , max(phonenumber_empty_t_f) as phonenumber_empty_t_f
               , max(throttling_failure_t_f) as throttling_failure_t_f
               , max(otp_success_signup) as otp_success_signup
               , max(otp_trigger_to_su_sec) as otp_trigger_to_su_sec
               from fduan.otp_dy_journey
               where dateint>=20181115
               group by 1) otp
      
      on alloc.alloc_account_id = otp.in_acct_id

left join fduan.otp_paid_after_signup_dedup pay --8025
      on alloc.alloc_account_id = pay.account_id
      and alloc.subscrn_id = pay.subscrn_id
      and alloc.cell_id = pay.cell_nbr

left join (select distinct account_id,cell_nbr
         from fduan.otp_chargebacks_after_signup) cb
      on alloc.alloc_account_id = cb.account_id
      and alloc.cell_id = cb.cell_nbr


left join (select account_id
         ,max(xborder_streaming_t_f) as xborder_streaming_t_f
         ,max(LATAM_stream_t_f) as LATAM_stream_t_f
         ,max(LATAM_signup_t_f) as LATAM_signup_t_f
         ,max(num_profiles) as num_profiles
         ,max(num_unique_titles) as num_unique_titles
         ,sum(total_view_min) as total_view_min
         ,sum(total_qualified_view_min) as total_qualified_view_min
         from fduan.otp_streaming_after_signup_cumulative_v2
         group by 1) pbf
      on alloc.alloc_account_id = pbf.account_id


left join fduan.otp_rev_after_signup_cumulative rev
      on alloc.alloc_account_id = rev.account_id
      and alloc.subscrn_id = rev.subscrn_id
      and alloc.cell_id = rev.cell_nbr

left join (select alloc_account_id
                 ,sum(contact_cnt) as total_cs_call
                 from fduan.otp_cs_contact
                 group by 1) cs 

      on alloc.alloc_account_id = cs.alloc_account_id

;



