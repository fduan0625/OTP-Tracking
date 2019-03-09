
INSERT OVERWRITE TABLE  fduan.otp_streaming_cntry_after_signup PARTITION (signup_utc_date,view_dateint)
	select  base.account_id
			,base.cell_nbr
			,base.pmt_decl_country_code
			,base.country_iso_code as signup_country
			,base.decl_pmt_mop_type 
			,otp.triggering_mop_mode
			,case when otp.in_acct_id is not null then 1 else 0 end as otp_signup_t_f
			,otp.phone_country_mismatch
			,otp.otp_phone_country_code

			,nf_to_unixtime_ms(base.signup_utc_ts) as signup_utc_ms

			,pbf.country_iso_code as streaming_country

			,pbf.profile_audio_language_iso_code
			,pbf.profile_text_language_iso_code
			,pbf.session_audio_language_iso_code  
			,pbf.session_text_language_iso_code
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
		and between nf_dateadd(nf_dateint_today() , -2) and nf_dateint_today()
		and view_dateint >= signup_utc_date

		;


	INSERT OVERWRITE TABLE fduan.otp_streaming_cntry_after_signup_cumu PARTITION (signup_utc_date,view_dateint)
	select 
       account_id,
       cell_nbr,
       pmt_decl_country_code,
       signup_country,
       decl_pmt_mop_type,
       triggering_mop_mode,
       otp_signup_t_f,
       phone_country_mismatch,
       otp_phone_country_code,
       streaming_country,
       profile_audio_language_iso_code,
       profile_text_language_iso_code,
       session_audio_language_iso_code,
       session_text_language_iso_code,
       count(*) as num_sessions,
	   min(signup_utc_date) as signup_utc_date,
	   max(view_dateint) as view_dateint

	 from fduan.otp_streaming_cntry_after_signup
	 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
;
