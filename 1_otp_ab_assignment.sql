SET TEST_START_DATEINT = 20181115;
SET TEST_END_DATEINT = 20190115; 
SET SIGNUP_REV_WINDOW = 63; -- only consider paid P2 revenue


-- CREATE TABLE if not exists fduan.otp_cell_assignment(
--        account_id        BIGINT,
--        cell_nbr          INT,
--        country           STRING,
--        alloc_utc_ms      BIGINT,
--        is_forced         INT

--    ) 
-- PARTITIONED by (dateint INT,hour INT) 
-- STORED AS PARQUET;

--- UTC date and hour
 INSERT OVERWRITE TABLE  fduan.otp_cell_assignment PARTITION(dateint,hour)
    select account_id
    ,cell_nbr
    ,country
    ,alloc_utc_ms
    ,cast(other_properties['is_forced'] as int) as is_forced
    ,dateint  --- UTC date 
    ,hour  
    from default.ab_non_member_events
    where event_type = 'Allocation'
    and test_id = 11440
    and 
    (
    (dateint = nf_dateint_today() and hour = nf_hour(nf_timestamp_now())) OR
    (dateint = nf_dateint(nf_timestamp_now() - interval '1' hour) and hour = nf_hour(nf_timestamp_now() - interval '1' hour))
    )
;



