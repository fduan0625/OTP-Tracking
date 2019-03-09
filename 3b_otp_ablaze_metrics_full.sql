CREATE TABLE if not exists fduan.otp_ablaze_metrics(
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
   cc_bin_number                        bigint

   ) 
PARTITIONED by (allocation_region_date INT) 
STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_ablaze_metrics PARTITION (allocation_region_date)
select distinct
   base.alloc_account_id
   , base.allocation_utc_date
   , base.subscrn_id
   , base.country_iso_code
   , base.cell_id 
   , base.test_id
   , base.other_properties['alloc_group_id']alloc_group_id
   , base.is_fraud
   , base.is_bot
   , base.device_category
   , base.allocation_utc_ts_ms
   , base.signup_utc_ts_ms
   , base.mop_utc_ts_ms
   , case when base.signup_utc_ts_ms is not null then 1 else 0 end is_signup
   , case when base.mop_utc_ts_ms is not null then 1 else 0 end is_mop_provided
   , nf_datediff('second',base.allocation_utc_ts_ms,base.signup_utc_ts_ms) secs_to_signup

  , cast(coalesce(base.other_properties['is_free_trial_at_signup'],'0') as int) as is_free_trial_at_signup
  , base.other_properties['displayWidthPixel'] as displayWidthPixel
  , base.other_properties['displayHeightPixel'] as displayHeightPixel
  , base.other_properties['browserName'] as browserName
 	, cast(base.retention_63d['signup_plan_rollup_id'] as int) as signup_plan_rollup_id
   
   , cast(coalesce(base.retention_63d['is_vol_cancel'],'0')as int) as p2_is_vol_cancel
   , cast(coalesce(base.retention_63d['is_invol_cancel'],'0')as int) as p2_is_invol_cancel
   , cast(coalesce(base.retention_63d['gross_realized_revenue'],'0')as double) as p2_gross_realized_revenue
   , cast(coalesce(base.retention_63d['net_realized_revenue'],'0')as double) as p2_net_realized_revenue
   , cast(coalesce(base.retention_63d['max_period_paid'],'0')as int) as max_period_paid_63d
   , cast(base.retention_63d['earliest_plan_change_date'] as INT)as p2_earliest_plan_change_date
   , cast(base.retention_63d['current_plan_rollup_id'] as int) as p2_current_plan_rollup_id
   , cast(coalesce(base.retention_63d['cumulative_streaming_secs'],'0') as int) as p2_streaming_secs



   , cast(coalesce(base.retention_98d['is_vol_cancel'],'0')as int)p3_is_vol_cancel
   , cast(coalesce(base.retention_98d['is_invol_cancel'],'0')as int)p3_is_invol_cancel
   , cast(coalesce(base.retention_98d['gross_realized_revenue'],'0')as double)p3_gross_realized_revenue
   , cast(coalesce(base.retention_98d['net_realized_revenue'],'0')as double)p3_net_realized_revenue
   , cast(coalesce(base.retention_98d['max_period_paid'],'0')as int)max_period_paid_98d
   , cast(base.retention_98d['earliest_plan_change_date'] as INT)as p3_earliest_plan_change_date
   , cast(base.retention_98d['current_plan_rollup_id'] as int) as p3_current_plan_rollup_id
   , cast(coalesce(base.retention_98d['cumulative_streaming_secs'],'0') as int) as p3_streaming_secs 
   , 1 alloc_cnt

   , a.event_utc_ms_ts as decl_utc_ms_ts
   , a.membership_status
   , a.processor_response
   , a.tmx_policy_score
   , a.mop_type
   , a.mop_institution
   , a.cc_bin_number
   , base.allocation_region_date
   from dse.ab_nm_alloc_f base
   inner join fduan.otp_pmt_event_hourly a  
		on base.account_id = a.account_id  ---- only consider those TMX score -50 to -80
		and a.row_num=1

   where base.allocation_region_date between nf_dateadd(nf_dateint_today() , -3) and nf_dateint_today() 
   and base.test_id = 11440
   and base.is_multicell = 0
   and base.deallocation_utc_ts_ms is null
  ;




CREATE TABLE if not exists fduan.otp_ablaze_metrics_pmt(
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
   allocation_region_date               INT,
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
   subscrn_d_fraud_flag                 INT

   ) 
PARTITIONED by (alloc_region_date INT) 
STORED AS PARQUET;







INSERT OVERWRITE TABLE fduan.otp_ablaze_metrics_pmt PARTITION (alloc_region_date)
select 
alloc.*
, case when bp1.first_try_pmt_type_desc is null and alloc.is_signup=1 then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
, case when bp1.first_try_brand_desc is null and alloc.is_signup=1 then 'Unknown'
    when bp1.first_try_brand_desc is not null then bp1.first_try_brand_desc
    else 'no_signup'
    end as signup_pmt_brand_desc
, case when alloc.is_signup=0 then 'no_signup'
    else coalesce(ig.grouped_pmt_institution_desc,inst.pmt_institution_desc,'UNKNOWN') end signup_issuer_desc_grp
, coalesce(mop.clear_bin, '--') signup_clear_bin
, coalesce(mop.bin_token, '--') signup_iban_bin

, case when bp2.first_try_pmt_type_desc is null and alloc.is_signup=1 then 'not_in_p2'
    when bp2.first_try_pmt_type_desc is not null then bp2.first_try_pmt_type_desc 
    else 'no_signup'
    end as p2_pmt_type_desc
, case when bp2.first_try_brand_desc is null and alloc.is_signup=1 then 'Unknown'
    when bp2.first_try_brand_desc is not null then bp2.first_try_brand_desc
    else 'no_signup'
    end as p2_pmt_brand_desc
, case when alloc.is_signup=0 then 'no_signup'
    else coalesce(p2ig.grouped_pmt_institution_desc,p2inst.pmt_institution_desc,'UNKNOWN') end p2_issuer_desc_grp
, coalesce(p2mop.clear_bin, '--') p2_clear_bin
, coalesce(p2mop.bin_token, '--') p2_iban_bin
, coalesce(subscrn_d.is_rejoin,0)is_rejoin
, coalesce(subscrn_d.fraud_flag,0)subscrn_d_fraud_flag
, allocation_region_date as alloc_region_date
from fduan.otp_ablaze_metrics alloc
left outer join dse.subscrn_d
    on alloc.alloc_account_id = subscrn_d.account_id
    and alloc.subscrn_id = subscrn_d.subscrn_id
left outer join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20181114) bp1
    on alloc.alloc_account_id = bp1.account_id
    and alloc.subscrn_id = bp1.subscrn_id
left outer join dse.pmt_mop_metadata_d mop
    on bp1.first_try_pmt_mop_metadata_sk = mop.pmt_mop_metadata_sk
left outer join dse.pmt_institution_v2_d inst
    on mop.pmt_institution_sk = inst.pmt_institution_sk
left outer join dse.pmt_institution_grouped_d  ig
    on (inst.pmt_institution_desc) = (ig.pmt_institution_desc)

left outer join (select * from dse.billing_period_end_f where billing_period_nbr = 2 and billing_period_end_date >=20181114) bp2
    on alloc.alloc_account_id = bp2.account_id
    and alloc.subscrn_id = bp2.subscrn_id
left outer join dse.pmt_mop_metadata_d p2mop
    on bp2.first_try_pmt_mop_metadata_sk = p2mop.pmt_mop_metadata_sk
left outer join dse.pmt_institution_v2_d p2inst
    on p2mop.pmt_institution_sk = p2inst.pmt_institution_sk
left outer join dse.pmt_institution_grouped_d  p2ig
    on (p2inst.pmt_institution_desc) = (p2ig.pmt_institution_desc)
;





INSERT OVERWRITE TABLE fduan.otp_ablaze_metrics_pmt PARTITION (alloc_region_date)
select 
alloc.*
, case when bp1.first_try_pmt_type_desc is null and alloc.is_signup=1 then 'Unknown'
    when bp1.first_try_pmt_type_desc is not null then bp1.first_try_pmt_type_desc 
    else 'no_signup'
    end as signup_pmt_type_desc
, case when bp1.first_try_brand_desc is null and alloc.is_signup=1 then 'Unknown'
    when bp1.first_try_brand_desc is not null then bp1.first_try_brand_desc
    else 'no_signup'
    end as signup_pmt_brand_desc
, case when alloc.is_signup=0 then 'no_signup'
    else coalesce(ig.grouped_pmt_institution_desc,inst.pmt_institution_desc,'UNKNOWN') end signup_issuer_desc_grp
, coalesce(mop.clear_bin, '--') signup_clear_bin
, coalesce(mop.bin_token, '--') signup_iban_bin

, case when bp2.first_try_pmt_type_desc is null and alloc.is_signup=1 then 'not_in_p2'
    when bp2.first_try_pmt_type_desc is not null then bp2.first_try_pmt_type_desc 
    else 'no_signup'
    end as p2_pmt_type_desc
, case when bp2.first_try_brand_desc is null and alloc.is_signup=1 then 'Unknown'
    when bp2.first_try_brand_desc is not null then bp2.first_try_brand_desc
    else 'no_signup'
    end as p2_pmt_brand_desc
, case when alloc.is_signup=0 then 'no_signup'
    else coalesce(p2ig.grouped_pmt_institution_desc,p2inst.pmt_institution_desc,'UNKNOWN') end p2_issuer_desc_grp
, coalesce(p2mop.clear_bin, '--') p2_clear_bin
, coalesce(p2mop.bin_token, '--') p2_iban_bin
, coalesce(subscrn_d.is_rejoin,0)is_rejoin
, coalesce(subscrn_d.fraud_flag,0)subscrn_d_fraud_flag
, allocation_region_date as alloc_region_date
from fduan.otp_ablaze_metrics alloc
left outer join dse.subscrn_d
    on alloc.alloc_account_id = subscrn_d.account_id
    and alloc.subscrn_id = subscrn_d.subscrn_id
left outer join (select * from dse.billing_period_end_f where billing_period_nbr = 1 and billing_period_end_date >=20181114) bp1
    on alloc.alloc_account_id = bp1.account_id
    and alloc.subscrn_id = bp1.subscrn_id
left outer join dse.pmt_mop_metadata_d mop
    on bp1.first_try_pmt_mop_metadata_sk = mop.pmt_mop_metadata_sk
left outer join dse.pmt_institution_v2_d inst
    on mop.pmt_institution_sk = inst.pmt_institution_sk
left outer join dse.pmt_institution_grouped_d  ig
    on (inst.pmt_institution_desc) = (ig.pmt_institution_desc)

left outer join (select * from dse.billing_period_end_f where billing_period_nbr = 2 and billing_period_end_date >=20181114) bp2
    on alloc.alloc_account_id = bp2.account_id
    and alloc.subscrn_id = bp2.subscrn_id
left outer join dse.pmt_mop_metadata_d p2mop
    on bp2.first_try_pmt_mop_metadata_sk = p2mop.pmt_mop_metadata_sk
left outer join dse.pmt_institution_v2_d p2inst
    on p2mop.pmt_institution_sk = p2inst.pmt_institution_sk
left outer join dse.pmt_institution_grouped_d  p2ig
    on (p2inst.pmt_institution_desc) = (p2ig.pmt_institution_desc)
;