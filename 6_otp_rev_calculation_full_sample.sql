/*** Calculate revenue gain except Germany ***/

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


, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then cast(nm.retention_63d['net_realized_revenue'] as double)*cast(nm.retention_63d['net_realized_revenue'] as double) else 0 end) as n_rev_63d_sqar
, sum(case when cb.account_id is null and nm.retention_63d['is_vol_cancel']='0' and nm.retention_63d['is_invol_cancel']='0' then cast(nm.retention_63d['gross_realized_revenue'] as double)*cast(nm.retention_63d['gross_realized_revenue'] as double) else 0 end) as g_rev_63d_sqar


from
dse.ab_nm_alloc_f nm
left join (select distinct account_id from fduan.otp_chargebacks_after_signup_full_alloc) cb
on nm.account_id = cb.account_id

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
group by 1,2;




/** Revenue Gain in Germany **/
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


, sum(case when  nm.retention_98d['is_vol_cancel']='0' and nm.retention_98d['is_invol_cancel']='0' then cast(nm.retention_98d['net_realized_revenue'] as double)*cast(nm.retention_98d['net_realized_revenue'] as double) else 0 end) as n_rev_98d_sqar
, sum(case when  nm.retention_98d['is_vol_cancel']='0' and nm.retention_98d['is_invol_cancel']='0' then cast(nm.retention_98d['gross_realized_revenue'] as double)*cast(nm.retention_98d['gross_realized_revenue'] as double) else 0 end) as g_rev_98d_sqar


from
dse.ab_nm_alloc_f nm

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

group by 1,2;