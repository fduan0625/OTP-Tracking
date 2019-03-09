 
-- Revenue daily level

   DROP TABLE IF EXISTS fduan.otp_rev_after_signup_daily;

   CREATE TABLE if not exists fduan.otp_rev_after_signup_daily(
       account_id		  	     			BIGINT,
       subscrn_id 				 			BIGINT,
       cell_nbr					 			INT,
       signup_date 				 			INT,
       billing_period_nbr 					INT,
       adjusted_revenue_amt_usd  			DOUBLE,
       gross_revenue_tax_inclusive_amt_usd  DOUBLE
   ) 
   PARTITIONED by (signup_utc_date INT,snapshot_date INT) 
   STORED AS PARQUET;


INSERT OVERWRITE TABLE fduan.otp_rev_after_signup_daily PARTITION (signup_utc_date,snapshot_date)
	select  base.account_id
			,base.subscrn_id
			,base.cell_nbr
			,base.signup_date
			,daily.billing_period_nbr
			,daily.adjusted_revenue_amt_usd
			,daily.gross_revenue_tax_inclusive_amt_usd
			,base.signup_utc_date
			,daily.snapshot_date
	from fduan.otp_signup_after_decline base
		left join dse.billing_account_day_d daily
		on daily.account_id=base.account_id 
		and daily.subscrn_id=base.subscrn_id
	where daily.snapshot_date>=20181114
	and daily.snapshot_date >= base.signup_date 
	and daily.snapshot_date <= nf_dateadd(base.signup_date,98)
;


-- Revenue daily cumulative level

   CREATE TABLE if not exists fduan.otp_rev_after_signup_cumulative(
       account_id		  	     			BIGINT,
       subscrn_id 				 			BIGINT,
       cell_nbr					 			INT,
       signup_date 				 			INT,
       average_paid_days_63d 				INT,
       average_paid_days_98d 				INT,
       net_realized_revenue_63d	 			DOUBLE,
       net_realized_revenue_98d	 			DOUBLE,
       gross_realized_revenue_63d 			DOUBLE,
       gross_realized_revenue_98d 			DOUBLE

   ) 
   PARTITIONED by (snapshot_date INT) 
   STORED AS PARQUET;




INSERT OVERWRITE TABLE fduan.otp_rev_after_signup_cumulative PARTITION (snapshot_date)
select 
	account_id
	,subscrn_id
	,cell_nbr
	,signup_date
	,sum(if(snapshot_date between signup_date 
		and nf_dateadd(signup_date,63) 
		and gross_revenue_tax_inclusive_amt_usd >0,1,0)) average_paid_days_63d
	,sum(if(snapshot_date between signup_date 
		and nf_dateadd(signup_date,98) 
		and gross_revenue_tax_inclusive_amt_usd >0,1,0)) average_paid_days_98d
	,sum(case when snapshot_date between signup_date 
		and nf_dateadd(signup_date,63) 
		then coalesce(adjusted_revenue_amt_usd,0) else 0 end) as net_realized_revenue_63d
	,sum(case when snapshot_date between signup_date 
		and nf_dateadd(signup_date,98) 
		then coalesce(adjusted_revenue_amt_usd,0) else 0 end) as net_realized_revenue_98d
	,sum(case when snapshot_date between signup_date 
		and nf_dateadd(signup_date,63) 
		then coalesce(gross_revenue_tax_inclusive_amt_usd,0) else 0 end) as gross_realized_revenue_63d
	,sum(case when snapshot_date between signup_date 
		and nf_dateadd(signup_date,98) 
		then coalesce(gross_revenue_tax_inclusive_amt_usd,0) else 0 end) as gross_realized_revenue_98d
	,max(snapshot_date) as snapshot_date
	from fduan.otp_rev_after_signup_daily 
	group by 1,2,3,4
;


/*** production code ***/



INSERT OVERWRITE TABLE fduan.otp_rev_after_signup_daily PARTITION (signup_utc_date,snapshot_date)
	select  base.account_id
			,base.subscrn_id
			,base.cell_nbr
			,base.signup_date
			,daily.billing_period_nbr
			,daily.adjusted_revenue_amt_usd
			,daily.gross_revenue_tax_inclusive_amt_usd
			,base.signup_utc_date
			,daily.snapshot_date
	from fduan.otp_signup_after_decline base
		left join dse.billing_account_day_d daily
		on daily.account_id=base.account_id 
		and daily.subscrn_id=base.subscrn_id
	where daily.snapshot_date between nf_dateadd(nf_dateint_today() , -3) and nf_dateint_today() 
	and daily.snapshot_date >= base.signup_date 
	and daily.snapshot_date <= nf_dateadd(base.signup_date,98)
;


INSERT OVERWRITE TABLE fduan.otp_rev_after_signup_cumulative PARTITION (snapshot_date)
select 
	account_id
	,subscrn_id
	,cell_nbr
	,signup_date
	,sum(if(snapshot_date between signup_date 
		and nf_dateadd(signup_date,63) 
		and gross_revenue_tax_inclusive_amt_usd >0,1,0)) average_paid_days_63d
	,sum(if(snapshot_date between signup_date 
		and nf_dateadd(signup_date,98) 
		and gross_revenue_tax_inclusive_amt_usd >0,1,0)) average_paid_days_98d
	,sum(case when snapshot_date between signup_date 
		and nf_dateadd(signup_date,63) 
		then coalesce(adjusted_revenue_amt_usd,0) else 0 end) as net_realized_revenue_63d
	,sum(case when snapshot_date between signup_date 
		and nf_dateadd(signup_date,98) 
		then coalesce(adjusted_revenue_amt_usd,0) else 0 end) as net_realized_revenue_98d
	,sum(case when snapshot_date between signup_date 
		and nf_dateadd(signup_date,63) 
		then coalesce(gross_revenue_tax_inclusive_amt_usd,0) else 0 end) as gross_realized_revenue_63d
	,sum(case when snapshot_date between signup_date 
		and nf_dateadd(signup_date,98) 
		then coalesce(gross_revenue_tax_inclusive_amt_usd,0) else 0 end) as gross_realized_revenue_98d
	,max(snapshot_date) as snapshot_date
	from fduan.otp_rev_after_signup_daily 
	group by 1,2,3,4
;

