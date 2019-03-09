   
-- fduan.otp_streaming_after_signup depend on
-- 3a_signup_after_decline.sql
-- fduan.otp_signup_after_decline refresh daily
-- dse.playback_session_f refresh daily

/**** RAW playback_session data ****/

   DROP TABLE IF EXISTS fduan.otp_streaming_after_signup;

   CREATE TABLE if not exists fduan.otp_streaming_after_signup(
       account_id		  	     BIGINT,
       cell_nbr					 INT,
       pmt_decl_country_code     STRING,
       decl_pmt_mop_type  		 STRING,
       triggering_mop_mode 		 STRING,
       otp_signup_t_f 			 INT,
       phone_country_mismatch 	 INT,
       carrier_type 			 STRING,
       voip_t_f 				 INT,
       signup_utc_ms	       	 BIGINT,
       profile_id 				 BIGINT,
       title_id 				 INT,
       view_utc_sec			     BIGINT,
       view_utc_ts				 TIMESTAMP,
       view_region_ts 			 TIMESTAMP,
       device_type_id			 INT,
       country_iso_code			 STRING,
       is_supplemental_playback  INT,
       standard_sanitized_duration_sec 	BIGINT,
       browse_sanitized_duration_sec	BIGINT,
       cumulative_sanitized_duration_sec	BIGINT,
       -- profile_audio_language_iso_code 		STRING,
       -- profile_text_language_iso_code 		STRING,
       -- session_audio_language_iso_code 		STRING,
       -- session_text_language_iso_code 		STRING


   ) 
   PARTITIONED by (signup_utc_date INT,view_dateint INT) 
   STORED AS PARQUET;



INSERT OVERWRITE TABLE  fduan.otp_streaming_after_signup PARTITION (signup_utc_date,view_dateint)
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
			-- ,pbf.profile_audio_language_iso_code
			-- ,pbf.profile_text_language_iso_code
			-- ,pbf.session_audio_language_iso_code  
			-- ,pbf.session_text_language_iso_code
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
		and view_dateint>=20181120   ---only runs on a daily level
		-- and view_dateint between nf_dateadd(nf_dateint_today(),-1) and nf_dateint_today()-- yesterday
		and view_dateint >= signup_utc_date
		-- and view_dateint between ${hiveconf:TEST_START_DATEINT} and nf_dateadd(${hiveconf:TEST_FULL_START_DATEINT}, ${hiveconf:REVENUE_BL_WINDOW})--- during test period. 

		;


  DROP TABLE IF EXISTS fduan.otp_streaming_after_signup_v2;

   CREATE TABLE if not exists fduan.otp_streaming_after_signup_v2(
       account_id		  	     BIGINT,
       cell_nbr					 INT,
       pmt_decl_country_code     STRING,
       decl_pmt_mop_type  		 STRING,
       triggering_mop_mode 		 STRING,
       otp_signup_t_f 			 INT,
       phone_country_mismatch 	 INT,
       carrier_type 			 STRING,
       voip_t_f 				 INT,
       signup_utc_ms	       	 BIGINT,
       profile_id 				 BIGINT,
       title_id 				 INT,
       view_utc_sec			     BIGINT,
       view_utc_ts				 TIMESTAMP,
       view_region_ts 			 TIMESTAMP,
       device_type_id			 INT,
       country_iso_code			 STRING,
       is_supplemental_playback  INT,
       standard_sanitized_duration_sec 	BIGINT,
       browse_sanitized_duration_sec	BIGINT,
       cumulative_sanitized_duration_sec	BIGINT,
       signup_utc_date                      INT
       -- profile_audio_language_iso_code 		STRING,
       -- profile_text_language_iso_code 		STRING,
       -- session_audio_language_iso_code 		STRING,
       -- session_text_language_iso_code 		STRING


   ) 
   PARTITIONED by (view_dateint INT) 
   STORED AS PARQUET;






/**** daily playback_session data ****/

   DROP TABLE IF EXISTS fduan.otp_streaming_after_signup_cumulative;

   CREATE TABLE if not exists fduan.otp_streaming_after_signup_cumulative(
       account_id		  	     BIGINT,
       cell_nbr					 INT,
       xborder_streaming_t_f     INT,
       LATAM_stream_t_f			 INT,
       LATAM_signup_t_f 		 INT,
       decl_pmt_mop_type  		 STRING,
       triggering_mop_mode 		 STRING,
       otp_signup_t_f 			 INT,
       phone_country_mismatch 	 INT,
       carrier_type 			 STRING,
       voip_t_f 				 INT,
       latest_signup_utc_sec     BIGINT,
       num_profiles 			 INT,
       num_unique_titles 		 INT,
       total_view_min  			 BIGINT,
       total_qualified_view_min  BIGINT
   ) 
   PARTITIONED by (signup_utc_date INT, view_dateint INT) 
   STORED AS PARQUET;


-- ,min((view_utc_sec - signup_utc_ms/1000)/3600/24) as signup2stream_day
	INSERT OVERWRITE TABLE fduan.otp_streaming_after_signup_cumulative PARTITION (signup_utc_date,view_dateint)
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

	from fduan.otp_streaming_after_signup
	group by 1,2,3,4,5,6,7,8,9,10,11
	;


  DROP TABLE IF EXISTS fduan.otp_streaming_after_signup_cumulative_v2;

   CREATE TABLE if not exists fduan.otp_streaming_after_signup_cumulative_v2(
       account_id		  	     BIGINT,
       cell_nbr					 INT,
       xborder_streaming_t_f     INT,
       LATAM_stream_t_f			 INT,
       LATAM_signup_t_f 		 INT,
       decl_pmt_mop_type  		 STRING,
       triggering_mop_mode 		 STRING,
       otp_signup_t_f 			 INT,
       phone_country_mismatch 	 INT,
       carrier_type 			 STRING,
       voip_t_f 				 INT,
       latest_signup_utc_sec     BIGINT,
       num_profiles 			 INT,
       num_unique_titles 		 INT,
       total_view_min  			 BIGINT,
       total_qualified_view_min  BIGINT,
       signup_utc_date 			 INT
   ) 
   PARTITIONED by (view_dateint INT) 
   STORED AS PARQUET;



/*** production ***/

INSERT OVERWRITE TABLE  fduan.otp_streaming_after_signup PARTITION (signup_utc_date,view_dateint)
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


	INSERT OVERWRITE TABLE fduan.otp_streaming_after_signup_cumulative PARTITION (signup_utc_date,view_dateint)
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

	from fduan.otp_streaming_after_signup
	group by 1,2,3,4,5,6,7,8,9,10,11
	;

 



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
