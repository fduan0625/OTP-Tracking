   
-- fduan.otp_streaming_after_signup depend on
-- 3a_signup_after_decline.sql
-- fduan.otp_signup_after_decline refresh daily
-- dse.playback_session_f refresh daily

/**** RAW playback_session data ****/

   DROP TABLE IF EXISTS fduan.otp_streaming_cntry_after_signup;

   CREATE TABLE if not exists fduan.otp_streaming_cntry_after_signup(
       account_id		  	     BIGINT,
       cell_nbr					 INT,
       pmt_decl_country_code     STRING,
       signup_country    		 STRING,
       decl_pmt_mop_type  		 STRING,
       triggering_mop_mode 		 STRING,
       otp_signup_t_f 			 INT,
       phone_country_mismatch 	 INT,
       otp_phone_country_code 	 STRING,
       signup_utc_ms	       	 BIGINT,
       streaming_country 		 STRING,
       profile_audio_language_iso_code 	 	STRING,
       profile_text_language_iso_code 		STRING,
       session_audio_language_iso_code 		STRING,
       session_text_language_iso_code 		STRING

   ) 
   PARTITIONED by (signup_utc_date INT,view_dateint INT) 
   STORED AS PARQUET;






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
		and view_dateint>=20181115   ---only runs on a daily level
		-- and view_dateint between nf_dateadd(nf_dateint_today(),-1) and nf_dateint_today()-- yesterday
		and view_dateint >= signup_utc_date
		-- and view_dateint between ${hiveconf:TEST_START_DATEINT} and nf_dateadd(${hiveconf:TEST_FULL_START_DATEINT}, ${hiveconf:REVENUE_BL_WINDOW})--- during test period. 

		;


/**** daily playback_session data ****/

   DROP TABLE IF EXISTS fduan.otp_streaming_cntry_after_signup_dedup;

   CREATE TABLE if not exists fduan.otp_streaming_cntry_after_signup_dedup(
       account_id		  	     BIGINT,
       cell_nbr					 INT,
       pmt_decl_country_code     STRING,
       signup_country    		 STRING,
       decl_pmt_mop_type  		 STRING,
       triggering_mop_mode 		 STRING,
       otp_signup_t_f 			 INT,
       phone_country_mismatch 	 INT,
       otp_phone_country_code 	 STRING,
       streaming_country 		 STRING,
       profile_audio_language_iso_code 	 	STRING,
       profile_text_language_iso_code 		STRING,
       session_audio_language_iso_code 		STRING,
       session_text_language_iso_code 		STRING,
       num_sessions 						INT

   ) 
   PARTITIONED by (signup_utc_date INT, update_dateint INT) 
   STORED AS PARQUET;


-- ,min((view_utc_sec - signup_utc_ms/1000)/3600/24) as signup2stream_day
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


/*** production ***/

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
