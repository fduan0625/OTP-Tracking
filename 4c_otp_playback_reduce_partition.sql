INSERT OVERWRITE TABLE  fduan.otp_streaming_after_signup_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup
	where signup_utc_date<=20181120
		;

INSERT OVERWRITE TABLE  fduan.otp_streaming_after_signup_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup
	where signup_utc_date between 20181121 and 20181125
		;

INSERT OVERWRITE TABLE  fduan.otp_streaming_after_signup_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup
	where signup_utc_date between 20181126 and 20181130
		;

INSERT OVERWRITE TABLE  fduan.otp_streaming_after_signup_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup
	where signup_utc_date between 20181201 and 20181205
		;

INSERT OVERWRITE TABLE  fduan.otp_streaming_after_signup_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup
	where signup_utc_date > 20181205
		;


/** for cumulative **/
INSERT OVERWRITE TABLE fduan.otp_streaming_after_signup_cumulative_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup_cumulative
	where view_dateint <=20181120
	;

INSERT OVERWRITE TABLE fduan.otp_streaming_after_signup_cumulative_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup_cumulative
	where view_dateint between 20181121 and 20181125
	;

INSERT OVERWRITE TABLE fduan.otp_streaming_after_signup_cumulative_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup_cumulative
	where view_dateint between 20181126 and 20181130
	;

INSERT OVERWRITE TABLE fduan.otp_streaming_after_signup_cumulative_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup_cumulative
	where view_dateint between 20181201 and 20181205
	;

INSERT OVERWRITE TABLE fduan.otp_streaming_after_signup_cumulative_v2 PARTITION (view_dateint)
	select * from fduan.otp_streaming_after_signup_cumulative
	where view_dateint > 20181205
	;
