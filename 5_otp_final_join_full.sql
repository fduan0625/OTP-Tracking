-- spark-sql --ver '2.1.1' -f path/to/query/file


-- select alloc_account_id as account_id
-- ,subscrn_id
-- ,country_iso_code
-- ,signup_pmt_type_desc
-- from fduan.otp_final_dup
-- where otp_success_signup  =1 

CREATE TABLE if not exists fduan.otp_final_dup(
   alloc_account_id                     BIGINT,
   allocation_utc_date                  INT,
   subscrn_id                           BIGINT,
   country_iso_code                     STRING,
   cell_id                              INT,
   test_id                              INT,
   alloc_group_id                       STRING,
   is_fraud                             INT,
   is_bot                               INT,
   device_category                      STRING,
   allocation_utc_ts_ms                 BIGINT,
   signup_utc_ts_ms                     BIGINT,
   mop_utc_ts_ms                        BIGINT,
   is_signup                            INT,
   is_mop_provided                      INT,
   secs_to_signup                       INT,
   is_free_trial_at_signup              INT,
   displayWidthPixel                    STRING,
   displayHeightPixel                   STRING,
   browserName                          STRING,
   signup_plan_rollup_id                INT,
   p2_is_vol_cancel                     INT,
   p2_is_invol_cancel                   INT,
   p2_gross_realized_revenue            DOUBLE,
   p2_net_realized_revenue              DOUBLE,
   max_period_paid_63d                  INT,
   p2_earliest_plan_change_date         INT,
   p2_current_plan_rollup_id            INT,
   p2_streaming_secs                    INT,
   p3_is_vol_cancel                     INT,
   p3_is_invol_cancel                   INT,
   p3_gross_realized_revenue            DOUBLE,
   p3_net_realized_revenue              DOUBLE,
   max_period_paid_98d                  INT,
   p3_earliest_plan_change_date         INT,
   p3_current_plan_rollup_id            INT,
   p3_streaming_secs                    INT,
   alloc_cnt                            INT,
   decl_utc_ms_ts                       BIGINT,
   membership_status                    STRING,
   processor_response                   STRING,
   tmx_policy_score                     INT,
   mop_type                             STRING,
   mop_institution                      STRING,
   cc_bin_number                        bigint,
   signup_pmt_type_desc                 STRING,
   signup_pmt_brand_desc                STRING,
   signup_issuer_desc_grp               STRING,
   signup_clear_bin                     STRING,
   signup_iban_bin                      STRING,
   p2_pmt_type_desc                     STRING,
   p2_pmt_brand_desc                    STRING,
   p2_issuer_desc_grp                   STRING,
   p2_clear_bin                         STRING,
   p2_iban_bin                          STRING,
   is_rejoin                            INT,
   subscrn_d_fraud_flag                 INT,
                           
   phone_country_mismatch               INT,
   resendcode_t_f                       INT,
   switch_pmt_t_f                       INT,
   user_initiate_back_t_f               INT,
   invalid_otp_t_f                      INT,
   retry_failure_t_f                    INT,
   pmt_failure_t_f                      INT,
   pmt_empty_t_f                        INT,
   phonenumber_empty_t_f                INT,
   throttling_failure_t_f               INT,
   otp_success_signup                   INT,
   otp_trigger_to_su_sec                INT,

   p1_vol_cancel_cnt                    INT,
   p1_invol_cancel_cnt                  INT,
   p1_onhold_cnt                        INT,
   p1_complete_cnt                      INT,
   p2_vol_cancel_cnt                    INT,
   p2_invol_cancel_cnt                  INT,
   p2_onhold_cnt                        INT,
   p2_complete_cnt                      INT,
   fraud_flag                           INT,
   paid_t_f                             INT,

   chargeback_t_f                       INT,

   xborder_streaming_t_f                INT,
   LATAM_stream_t_f                     INT,
   LATAM_signup_t_f                     INT,
   num_profiles                         INT,
   num_unique_titles                    INT,
   total_view_min                       INT,
   total_qualified_view_min             INT,

   average_paid_days_63d                INT,
   average_paid_days_98d                INT,
   net_realized_revenue_63d             INT,
   net_realized_revenue_98d             INT,
   gross_realized_revenue_63d           INT,
   gross_realized_revenue_98d           INT,
   ever_contact_cs_t_f                  INT,
   total_cs_call                        INT

   ) 
PARTITIONED by (allocation_region_date INT) 
STORED AS PARQUET;




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




/**** queries in tableau *****/
-- select mop_type,processor_response,cell_id,min(tmx_policy_score),max(tmx_policy_score),count(*) from fduan.otp_final_dup
-- group by 1,2,3;
-- why some are not triggered OTP in cell 2?


select cell_id, mop_type,signup_pmt_type_desc,count(*)
from fduan.otp_final_dup
where is_signup=1
group by 1,2,3;

/**** AB main metrics ****/
select 
nf_timestamp(allocation_region_date) as allocation_region_date
,country_iso_code
,cell_id
,mop_type as decl_mop_type
,case when signup_pmt_type_desc in ('Credit','Debit') then 'card'
      when signup_pmt_type_desc in ('Prepaid') then 'Prepaid'
      when signup_pmt_type_desc in ('Gift Card') then 'Gift Card'
      when signup_pmt_type_desc in ('Paypal') then 'Paypal'
      when signup_pmt_type_desc in ('Direct Debit') then 'Direct Debit'
      when signup_pmt_type_desc in ('ITUNES') then 'ITUNES'
      else 'others' end as signup_pmt_type
,membership_status
,case   when tmx_policy_score <=-70 then '-80 ~ -70'
      when tmx_policy_score <=-60 then '-70 ~ -60'
      when tmx_policy_score <=-50 then '-60 ~ -50'
      end as tmx_score_band
,sum(alloc_cnt) as sum_allocatons

,count(distinct alloc_account_id) as num_unique_accts
,sum(is_signup) as sum_signup
,sum(otp_success_signup) as sum_otp_signup
,sum(case when is_free_trial_at_signup<>1 then is_signup else 0 end) as sum_paid_signup
,sum(case when secs_to_signup/1000 > 300 then 300
          when secs_to_signup/1000 is null then 0
          else secs_to_signup/1000 end) as secs_to_signup

,sum(case when is_fraud<>1 then is_signup else 0 end) as sum_non_fraud_signup
,sum(case when is_fraud<>1 and xborder_streaming_t_f<>1 then is_signup else 0 end) as sum_non_fraud_non_xborder_signup

,sum(p2_net_realized_revenue) as p2_net_realized_revenue
,sum(case when p2_is_invol_cancel<>1 and p2_is_vol_cancel<>1 and is_fraud<>1 and chargeback_t_f<>1 then p2_net_realized_revenue else 0 end) as adj_p2_net_realized_revenue
,sum(p2_gross_realized_revenue) as p2_gross_realized_revenue
,sum(p2_is_vol_cancel) as p2_is_vol_cancel
,sum(p2_is_invol_cancel) as p2_is_invol_cancel
,sum(p2_streaming_secs) as p2_streaming_secs
,sum(case when p2_is_invol_cancel<>1 and p2_is_vol_cancel<>1 and is_fraud<>1 and chargeback_t_f<>1 then paid_t_f else 0 end) as adjust_sum_paid_p2
,sum(average_paid_days_63d) as  sum_paid_days_63d 

,sum(p3_net_realized_revenue) as p3_net_realized_revenue
,sum(case when p3_is_invol_cancel<>1 and p3_is_vol_cancel<>1 and is_fraud<>1 and chargeback_t_f<>1 then p3_net_realized_revenue else 0 end) as adj_p3_net_realized_revenue
,sum(p3_gross_realized_revenue) as p3_gross_realized_revenue
,sum(p3_is_vol_cancel) as p3_is_vol_cancel
,sum(p3_is_invol_cancel) as p3_is_invol_cancel
,sum(p3_streaming_secs) as p3_streaming_secs
,sum(case when p3_is_invol_cancel<>1 and p3_is_vol_cancel<>1 and is_fraud<>1 and chargeback_t_f<>1 then paid_t_f else 0 end) as adjust_sum_paid_p3
,sum(average_paid_days_98d) as  sum_paid_days_98d 

,sum(paid_t_f) as sum_paid
,sum(chargeback_t_f) as sum_chargeback
,sum(ever_contact_cs_t_f) as cs_contact_cnt
,sum(total_cs_call) as cs_contact_sum

from fduan.otp_final_dup
where is_bot !=1
group by 1,2,3,4,5,6,7


/**** CS contact throttle failure ****/
select 
nf_timestamp(allocation_region_date) as allocation_region_date
,cell_id
,country_iso_code
,mop_type as decl_mop_type
,case when xborder_streaming_t_f=1 and LATAM_stream_t_f=1 and LATAM_signup_t_f<>1 then 1 else 0 end as xborder_latam_stream
,sum(invalid_otp_t_f) as invalid_otp
,sum(throttling_failure_t_f) as throttle_failure
,sum(coalesce(ever_contact_cs_t_f,0)) as contact_cs_acct
,sum(case when throttling_failure_t_f=1 then ever_contact_cs_t_f else 0 end) as throttle_contact_cs
,sum(case when throttling_failure_t_f=1 and is_fraud=1 then ever_contact_cs_t_f else 0 end) as throttle_fraud_contact_cs
,sum(case when throttling_failure_t_f=1 and is_signup=1 then ever_contact_cs_t_f else 0 end) as throttle_contact_cs_signup
,sum(case when throttling_failure_t_f=1 and is_signup=1 then 1 else 0 end) as throttle_signup

from fduan.otp_final_dup
where is_bot !=1 and cell_id = 2
group by 1,2,3,4



select cell_id
,is_bot
,sum(alloc_cnt) as sum_allocatons
,sum(is_signup) as num_signup
,sum(otp_success_signup) as otp_signup
,sum(ever_contact_cs_t_f) as cs
,sum(throttling_failure_t_f) as throttle
,sum(case when throttling_failure_t_f=1 then ever_contact_cs_t_f else 0 end) as throttle_cs
,sum(case when throttling_failure_t_f=1 and ever_contact_cs_t_f = 1 then is_signup else 0 end) as throttle_cs_signup
,sum(case when throttling_failure_t_f=1 then is_signup else 0 end) as throttle_signup

from fduan.otp_final_dup
group by 1,2;


/**** phone country mismatch + xborder signup ****/
select 
-- nf_timestamp(allocation_region_date) as allocation_region_date
nf_date(signup_utc_ts_ms) as signup_utc_date
,phone_country_mismatch
,xborder_streaming_t_f 
,case when xborder_streaming_t_f=1 and LATAM_stream_t_f =1 and LATAM_signup_t_f != 1 then 1 else 0 end as xborder_latam_streaming_t_f
,mop_type as decl_mop_type
,sum(otp_success_signup) as sum_otp_signup
from fduan.otp_final_dup
where otp_success_signup=1
group by 1,2,3,4,5





/**** AB main metrics AB comparison ****/
select 
nf_timestamp(allocation_region_date) as allocation_region_date

-- nf_date(signup_utc_ts_ms) as signup_utc_date
,country_iso_code
,cell_id
,mop_type as decl_mop_type
,case when signup_pmt_type_desc in ('Credit','Debit') then 'card'
      when signup_pmt_type_desc in ('Prepaid') then 'Prepaid'
      when signup_pmt_type_desc in ('Gift Card') then 'Gift Card'
      when signup_pmt_type_desc in ('Paypal') then 'Paypal'
      when signup_pmt_type_desc in ('Direct Debit') then 'Direct Debit'
      when signup_pmt_type_desc in ('ITUNES') then 'ITUNES'
      else 'others' end as signup_pmt_type
,membership_status
,case   when tmx_policy_score <=-70 then '-80 ~ -70'
      when tmx_policy_score <=-60 then '-70 ~ -60'
      when tmx_policy_score <=-50 then '-60 ~ -50'
      end as tmx_score_band

,sum(alloc_cnt) as a_sum_allocatons
,sum(case when cell_id = 1 then alloc_cnt else 0 end) as a_sum_allocatons_1
,sum(case when cell_id = 2 then alloc_cnt else 0 end) as a_sum_allocatons_2

,sum(is_signup) as b_sum_signup
,sum(case when cell_id = 1 then is_signup else 0 end) as b_sum_signup_1
,sum(case when cell_id = 2 then is_signup else 0 end) as b_sum_signup_2

,sum(otp_success_signup) as sum_otp_signup

,sum(case when is_fraud<>1 then is_signup else 0 end) as c_sum_non_fraud_signup
,sum(case when is_fraud<>1 and cell_id=1 then is_signup else 0 end) as c_sum_non_fraud_signup_1
,sum(case when is_fraud<>1 and cell_id=2 then is_signup else 0 end) as c_sum_non_fraud_signup_2

,sum(case when is_fraud<>1 and xborder_streaming_t_f<>1 then is_signup else 0 end) as d_sum_non_fraud_non_xborder_signup
,sum(case when cell_id=1 and is_fraud<>1 and xborder_streaming_t_f<>1 then is_signup else 0 end) as d_sum_non_fraud_non_xborder_signup_1
,sum(case when cell_id=2 and is_fraud<>1 and xborder_streaming_t_f<>1 then is_signup else 0 end) as d_sum_non_fraud_non_xborder_signup_2

,sum(p2_net_realized_revenue) as e_p2_net_realized_revenue
,sum(case when cell_id=1 then p2_net_realized_revenue else 0 end) as e_p2_net_realized_revenue_1
,sum(case when cell_id=2 then p2_net_realized_revenue else 0 end) as e_p2_net_realized_revenue_2

,sum(case when p2_is_invol_cancel<>1 and xborder_streaming_t_f<>1 and is_fraud<>1 and chargeback_t_f<>1 then p2_net_realized_revenue else 0 end) as f_adj_p2_net_realized_revenue
,sum(case when cell_id=1 and p2_is_invol_cancel<>1 and xborder_streaming_t_f<>1 and is_fraud<>1 and chargeback_t_f<>1 then p2_net_realized_revenue else 0 end) as f_adj_p2_net_realized_revenue_1
,sum(case when cell_id=2 and p2_is_invol_cancel<>1 and xborder_streaming_t_f<>1 and is_fraud<>1 and chargeback_t_f<>1 then p2_net_realized_revenue else 0 end) as f_adj_p2_net_realized_revenue_2


,sum(total_cs_call) as g_cs_contact_sum
,sum(case when cell_id=1 then total_cs_call else 0 end) as g_cs_contact_sum_1
,sum(case when cell_id=2 then total_cs_call else 0 end) as g_cs_contact_sum_2

from fduan.otp_final_dup
where is_bot !=1
group by 1,2,3,4,5,6,7


/** IBAN BIN level deep dive **/

select 
signup_issuer_desc_grp
,signup_iban_bin
,signup_clear_bin
,count(*)
from fduan.otp_ablaze_metrics_pmt
group by 1,2,3;


select 

a.phone_country_mismatch
,a.mop_type
,a.signup_pmt_type_desc
,a.signup_pmt_brand_desc
,b.signup_issuer_desc_grp as cc_signup_issuer_desc_grp
,a.signup_clear_bin
,a.xborder_streaming_t_f
,count(distinct alloc_account_id) as num_signup
from fduan.otp_final_dup a
   left join (select 
      signup_issuer_desc_grp
      ,signup_clear_bin
      ,count(*)
      from fduan.otp_ablaze_metrics_pmt
      group by 1,2)b
   on a.signup_clear_bin = b.signup_clear_bin

   left join (select 
      signup_issuer_desc_grp
      ,signup_iban_bin
      ,count(*)
      from fduan.otp_ablaze_metrics_pmt
      group by 1,2)c
   on UPPER(a.mop_institution) = UPPER(c.signup_iban_bin)
where is_signup=1
and cell_id=2
group by 1,2,3,4,5,6,7




select 
a.phone_country_mismatch
,a.mop_type
,a.mop_institution
,a.signup_pmt_type_desc
,a.signup_pmt_brand_desc
,c.signup_issuer_desc_grp as dd_signup_issuer_desc_grp
,a.signup_iban_bin
,a.xborder_streaming_t_f
,count(distinct alloc_account_id) as num_signup
from fduan.otp_final_dup a

   left join (select 
      signup_issuer_desc_grp
      ,signup_iban_bin
      ,count(*)
      from fduan.otp_ablaze_metrics_pmt
      group by 1,2)c
   on UPPER(a.mop_institution) = UPPER(c.signup_iban_bin)
where is_signup=1
and cell_id=2
and UPPER(signup_iban_bin) like 'DE-%'
group by 1,2,3,4,5,6,7,8


select alloc_account_id from fduan.otp_final_dup 
where is_signup=1
and cell_id=2
and country_iso_code = 'DE'
and phone_country_mismatch=1
and allocation_utc_date=20181202
and xborder_streaming_t_f = 0

limit 10;









select
nf_timestamp(allocation_region_date) as allocation_region_date
,cell_id
,count(*) as num_alloc
, sum(case when signup_utc_ts_ms is not null then 1 else 0 end) as num_signup

, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='1' then 1 else 0 end) as cell1_63d_vol
, sum(case when cb.account_id is null and nm.retention_63d['is_invol_cancel']='1' then 1 else 0 end) as cell1_63d_invol

,sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' and nm.retention_63d['current_plan_rollup_id']='3108' then 13.99
when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' and nm.retention_63d['current_plan_rollup_id']='3088' then 10.99
when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' and nm.retention_63d['current_plan_rollup_id']='4001' then 9.99 else 0 end) as total_asp_p3

, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then cast(nm.retention_63d['net_realized_revenue'] as double) else 0 end) as n_rev_63d
, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then cast(nm.retention_63d['gross_realized_revenue'] as double) else 0 end) as g_rev_63d

, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then
 (CASE WHEN adjusted_monthly_rev > 0 THEN adjusted_monthly_rev ELSE price END)*est_tenure_36m else 0 end) as ltv_36m

, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then
 (CASE WHEN adjusted_monthly_rev > 0 THEN adjusted_monthly_rev ELSE price END)*est_tenure_24m else 0 end) as ltv_24m

, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then
 (CASE WHEN adjusted_monthly_rev > 0 THEN adjusted_monthly_rev ELSE price END)*est_tenure_12m else 0 end) as ltv_12m

, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then cast(nm.retention_63d['net_realized_revenue'] as double)*cast(nm.retention_63d['net_realized_revenue'] as double) else 0 end) as n_rev_63d_sqar
, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then cast(nm.retention_63d['gross_realized_revenue'] as double)*cast(nm.retention_63d['gross_realized_revenue'] as double) else 0 end) as g_rev_63d_sqar


from
dse.ab_nm_alloc_f nm
   left join (select distinct account_id from fduan.otp_chargebacks_after_signup_full_alloc) cb
on nm.account_id = cb.account_id
left join (SELECT
   account_id,
   subscrn_id,
   est_tenure_36m,
   est_tenure_24m,
   est_tenure_12m,
   CAST(json_extract(json_parse(other_properties),'$.adjusted_rev') as double) as adjusted_monthly_rev,
   CAST(json_extract(json_parse(other_properties),'$.price') as double) as price

   FROM
   etl.pcltv_account_prediction_f
   WHERE
   other_properties not like '%NaN%' AND
   snapshot_dateint between 20181015 and 20181217 AND
   tenure_days =1 AND
   model_version = 'v2'
   ) a

on nm.account_id = a.account_id
and nm.subscrn_id = a.subscrn_id


where
test_id = 11440
and
country_iso_code <> 'DE'

and
coalesce(is_fraud, -1) in (0, -1)
and
coalesce(is_bot, -1) in (0, -1)
and
membership_status != 2
and cb.account_id is null
group by 1,2










select
nf_timestamp(allocation_region_date) as allocation_region_date
,cell_id
,count(*) as num_alloc
, sum(case when signup_utc_ts_ms is not null then 1 else 0 end) as num_signup

, sum(case when  nm.retention_98d['is_vol_cancel']='1' then 1 else 0 end) as cell1_98d_vol
, sum(case when  nm.retention_98d['is_invol_cancel']='1' then 1 else 0 end) as cell1_98d_invol

,sum(case when  nm.retention_98d['is_vol_cancel']='0' and nm.retention_98d['is_invol_cancel']='0' and nm.retention_98d['current_plan_rollup_id']='3108' then 13.99
when  nm.retention_98d['is_vol_cancel']='0' and nm.retention_98d['is_invol_cancel']='0' and nm.retention_98d['current_plan_rollup_id']='3088' then 10.99
when  nm.retention_98d['is_vol_cancel']='0' and nm.retention_98d['is_invol_cancel']='0' and nm.retention_98d['current_plan_rollup_id']='4001' then 9.99 else 0 end) as total_asp_p3

, sum(case when  nm.retention_98d['is_vol_cancel']='0' and nm.retention_98d['is_invol_cancel']='0' then cast(nm.retention_98d['net_realized_revenue'] as double) else 0 end) as n_rev_98d
, sum(case when  nm.retention_98d['is_vol_cancel']='0' and nm.retention_98d['is_invol_cancel']='0' then cast(nm.retention_98d['gross_realized_revenue'] as double) else 0 end) as g_rev_98d

, sum(case when nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then
 (CASE WHEN adjusted_monthly_rev > 0 THEN adjusted_monthly_rev ELSE price END)*est_tenure_36m else 0 end) as ltv_36m

, sum(case when nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then
 (CASE WHEN adjusted_monthly_rev > 0 THEN adjusted_monthly_rev ELSE price END)*est_tenure_24m else 0 end) as ltv_24m

, sum(case when nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then
 (CASE WHEN adjusted_monthly_rev > 0 THEN adjusted_monthly_rev ELSE price END)*est_tenure_12m else 0 end) as ltv_12m

, sum(case when  nm.retention_98d['is_vol_cancel']='0' and nm.retention_98d['is_invol_cancel']='0' then cast(nm.retention_98d['net_realized_revenue'] as double)*cast(nm.retention_98d['net_realized_revenue'] as double) else 0 end) as n_rev_98d_sqar
, sum(case when  nm.retention_98d['is_vol_cancel']='0' and nm.retention_98d['is_invol_cancel']='0' then cast(nm.retention_98d['gross_realized_revenue'] as double)*cast(nm.retention_98d['gross_realized_revenue'] as double) else 0 end) as g_rev_98d_sqar


from
dse.ab_nm_alloc_f nm
left join (SELECT
   account_id,
   subscrn_id,
   est_tenure_36m,
   est_tenure_24m,
   est_tenure_12m,
   CAST(json_extract(json_parse(other_properties),'$.adjusted_rev') as double) as adjusted_monthly_rev,
   CAST(json_extract(json_parse(other_properties),'$.price') as double) as price

   FROM
   etl.pcltv_account_prediction_f
   WHERE
   other_properties not like '%NaN%' AND
   snapshot_dateint between 20181015 and 20181217 AND
   tenure_days =1 AND
   model_version = 'v2'
   ) a

on nm.account_id = a.account_id
and nm.subscrn_id = a.subscrn_id
where
test_id = 11440
and
country_iso_code = 'DE'

and
coalesce(is_fraud, -1) in (0, -1)
and
coalesce(is_bot, -1) in (0, -1)
and
membership_status != 2

group by 1,2
