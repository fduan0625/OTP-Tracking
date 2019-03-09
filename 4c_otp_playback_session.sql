INSERT OVERWRITE TABLE  fduan.otp_streaming_after_signup_v2 PARTITION (view_dateint)
	select  base.account_id
			,base.cell_nbr
			,base.pmt_decl_country_code
			,base.decl_pmt_mop_type 
			,otp.triggering_mop_mode
			,case when otp.in_acct_id is not null then 1 else 0 end as otp_signup_t_f
			,otp.phone_country_mismatch
			,otp.carrier_type
			,otp.voip_t_f
			,nf_to_unixtime_ms(base.signup_utc_ts) as signup_utc_ms
			,pbf.profile_id
			,pbf.title_id
			,pbf.view_utc_sec
			,nf_from_unixtime(pbf.view_utc_sec) view_utc_ts
			,pbf.event_region_ts as view_region_ts
			,pbf.device_type_id
			,pbf.country_iso_code
			,pbf.is_supplemental_playback
			,pbf.standard_sanitized_duration_sec
			,pbf.browse_sanitized_duration_sec
			,pbf.cumulative_sanitized_duration_sec
			,base.signup_utc_date
			,pbf.view_dateint

	from fduan.otp_signup_after_decline base
	left join dse.playback_session_f pbf
		on base.account_id = pbf.account_id
	left join fduan.otp_dy_journey  otp
		on base.account_id = otp.in_acct_id
		and otp.otp_success_signup=1

	where is_supplemental_playback=0 --- trailors
		and standard_sanitized_duration_sec > browse_sanitized_duration_sec and standard_sanitized_duration_sec > 120 -- user engaged streaming
		and view_dateint between nf_dateadd(nf_dateint_today() , -2) and nf_dateint_today() 
 		and view_dateint >= signup_utc_date

		;

 INSERT OVERWRITE TABLE fduan.otp_streaming_after_signup_cumulative_v2 PARTITION (view_dateint)
	select account_id
		,cell_nbr
		,case when pmt_decl_country_code!=country_iso_code then 1 else 0 end as xborder_streaming_t_f
		,(case when country_iso_code in ('AG','AI','AN','AR','AW','BB','BL','BM','BO','BQ','BR','BS','BZ','CL','CO','CR','CU','CW','DM','DO','EC','FK','GD','GF','GP','GS','GT','GY','HN','HT','JM','KN','KY','LC','MF','MQ','MS','MX','NI','PA','PE','PY','SR','SV','SX','TC','TT','UY','VC','VE','VG') then 1 else 0 end) as LATAM_stream_t_f
		,(case when pmt_decl_country_code in ('AG','AI','AN','AR','AW','BB','BL','BM','BO','BQ','BR','BS','BZ','CL','CO','CR','CU','CW','DM','DO','EC','FK','GD','GF','GP','GS','GT','GY','HN','HT','JM','KN','KY','LC','MF','MQ','MS','MX','NI','PA','PE','PY','SR','SV','SX','TC','TT','UY','VC','VE','VG') then 1 else 0 end) as LATAM_signup_t_f
		,decl_pmt_mop_type 
		,triggering_mop_mode
		,otp_signup_t_f
		,phone_country_mismatch
		,carrier_type
		,voip_t_f 	
		,max(signup_utc_ms/1000) as latest_signup_utc_sec
		,count(distinct profile_id) as num_profiles
		,count(distinct title_id) as num_unique_titles
		,sum(standard_sanitized_duration_sec)/60 as total_view_min 
		,sum(case when standard_sanitized_duration_sec>=360 then standard_sanitized_duration_sec else 0 end)/60 as total_qualified_view_min ----- >= 6 min as qualified play
		,min(signup_utc_date) as signup_utc_date
		,min(view_dateint) as view_dateint

	from fduan.otp_streaming_after_signup_v2
	where signup_utc_date between nf_dateadd(nf_dateint_today() , -14) and nf_dateint_today()
	group by 1,2,3,4,5,6,7,8,9,10,11
	;