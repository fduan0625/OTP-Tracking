-- fduan.otp_paid_after_signup depend on
-- 3a_signup_after_decline.sql script
-- fduan.otp_signup_after_decline refresh daily
-- dse.billing_period_end_f  refreshed daily
-- schedule in the daily refresh job

drop table if exists fduan.otp_paid_after_signup;
CREATE TABLE if not exists fduan.otp_paid_after_signup(
		account_id 					BIGINT,
		cell_nbr 					INT,
		is_free_trial_at_signup 	INT,
		decl_pmt_mop_type 			STRING,
		subscrn_id 					BIGINT,
		mop_id 						BIGINT,
		billing_period_nbr 			INT,
		country_iso_code 			STRING,
		possible_complete_cnt		INT,
		complete_cnt 				INT,
		vol_cancel_cnt 				INT,
		invol_cancel_cnt 			INT,
		onhold_cnt 					INT,
		is_tester_account 			INT,
		is_free_trial 				INT,
		device_fraud_hold_cnt 		INT,
		fraud_flag 					INT,
		current_plan_id 			INT,
		next_period_plan_id 		INT,
		billing_period_auth_code 	STRING,
		paid_t_f 					INT

   ) 
PARTITIONED by (billing_period_end_date INT,signup_utc_date INT) 
STORED AS PARQUET;

/*** Out of sign up how many paid ***/
INSERT OVERWRITE TABLE fduan.otp_paid_after_signup PARTITION(billing_period_end_date)
	select base.account_id
			,a.cell_nbr
			,a.is_free_trial_at_signup
			,a.decl_pmt_mop_type
		    ,base.subscrn_id
		    ,base.mop_id
		    ,base.billing_period_nbr
		    ,base.country_iso_code
		    ,base.possible_complete_cnt  ---churn denominator
		    ,base.complete_cnt  ---churn numerator
		    ,base.vol_cancel_cnt
		    ,base.invol_cancel_cnt
		    ,base.onhold_cnt
		    ,base.is_tester_account
		    ,base.is_free_trial
		    ,base.device_fraud_hold_cnt
		    ,base.fraud_flag
		    ,base.current_plan_id
		    ,base.next_period_plan_id
		    ,base.billing_period_auth_code
		    ---- dse.billing_period_auth_code_d
		    -- If they are not in free trial and their auth date is not null then you can consider them as P1 Paid
		    ,case   when base.billing_period_nbr >=2 and base.billing_period_auth_date is not null and base.billing_period_auth_code = 'P' then 1
		    		when base.billing_period_nbr = 1 and base.is_free_trial = 0 and base.billing_period_auth_date is not null then 1
		    		else 0 end as paid_t_f
		    ,base.billing_period_end_date
		    ,a.signup_utc_date
		from dse.billing_period_end_f base
			inner join fduan.otp_signup_after_decline a
			on base.account_id = a.account_id
			and base.subscrn_id=a.subscrn_id
		where base.billing_period_nbr<=3   --- For DE Direct Debit, wait for P3 as it takes 10 days to process at P2
		and base.billing_period_end_date>=20181114
		and a.row_num=1
		;



CREATE TABLE if not exists fduan.otp_paid_after_signup_dedup (
		account_id 					BIGINT,
		subscrn_id 					BIGINT,
		cell_nbr 					INT,
		is_free_trial_at_signup 	INT,
		decl_pmt_mop_type 			STRING,
		country_iso_code 			STRING,
		p1_vol_cancel_cnt 			INT,
		p1_invol_cancel_cnt 		INT,
		p1_nondevice_onhold_cnt 	INT,
		p1_onhold_cnt 				INT,
		p1_complete_cnt 			INT,
		p2_vol_cancel_cnt 			INT,
		p2_invol_cancel_cnt 		INT,
		p2_nondevice_onhold_cnt 	INT,
		p2_onhold_cnt 				INT,
		p2_complete_cnt 			INT,
		fraud_flag 					INT,
		paid_t_f 					INT

   ) 
PARTITIONED by (signup_utc_date INT) 
STORED AS PARQUET;




-- order : invol_cancel , vol_cancel, paid and complete
INSERT OVERWRITE TABLE fduan.otp_paid_after_signup_dedup PARTITION (signup_utc_date)
	select account_id
	,subscrn_id
	,cell_nbr
	,is_free_trial_at_signup
	,decl_pmt_mop_type
	,country_iso_code
	,max(case when billing_period_nbr = 1 then vol_cancel_cnt else 0 end) as p1_vol_cancel_cnt
	,max(case when billing_period_nbr = 1 then invol_cancel_cnt else 0 end) as p1_invol_cancel_cnt
	,max(case when billing_period_nbr = 1 and onhold_cnt = 1 and device_fraud_hold_cnt != 1 then 1 else 0 end) as p1_nondevice_onhold_cnt
	,max(case when billing_period_nbr = 1 then onhold_cnt else 0 end) as p1_onhold_cnt
	,max(case when billing_period_nbr = 1 then complete_cnt else 0 end) as p1_complete_cnt
	,max(case when billing_period_nbr in (2,3) then vol_cancel_cnt else 0 end) as p2_vol_cancel_cnt
	,max(case when billing_period_nbr in (2,3) then invol_cancel_cnt else 0 end) as p2_invol_cancel_cnt
	,max(case when billing_period_nbr in (2,3) and onhold_cnt = 1 and device_fraud_hold_cnt != 1 then 1 else 0 end) as p2_nondevice_onhold_cnt
	,max(case when billing_period_nbr in (2,3) then onhold_cnt else 0 end) as p2_onhold_cnt
	,max(case when billing_period_nbr in (2,3) then complete_cnt else 0 end) as p2_complete_cnt
	,max(fraud_flag) as fraud_flag
	,max(paid_t_f) as paid_t_f
	,max(signup_utc_date) as signup_utc_date
from fduan.otp_paid_after_signup
where possible_complete_cnt=1
group by 1,2,3,4,5,6;

