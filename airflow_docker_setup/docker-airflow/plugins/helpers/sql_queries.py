class SqlQueries:
	session_insert = ("""
		INSERT INTO victory.sessions (user_id, session_id, app_ver, session_start, session_end, country, session_purchases,session_value)
		SELECT  
		user_id
		, session_id
		, app_ver
		, MIN (TIMESTAMP 'epoch' + (event_timestamp /1000) * INTERVAL '1 second') AS session_start 
		, MAX (TIMESTAMP 'epoch' + (event_timestamp /1000) * INTERVAL '1 second') AS session_end
		, country
		, MAX(session_purchases) as session_purchases
		, MAX(session_value) AS session_value
		FROM victory.raw_events
		GROUP BY 
		user_id
		, session_id
		, app_ver
		, country;""")

	play_end_insert = ("""
		INSERT INTO victory.play_end
		SELECT  user_id
       ,  session_id
       , event_id
       , (TIMESTAMP 'epoch' + (event_timestamp /1000) * INTERVAL '1 second') AS event_timestamp 
       , 'PlayEnd' AS event_name 
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playEnd', 'matchId') AS match_id 
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playEnd', 'court') AS court
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playEnd', 'opponentId') AS opponent_id 
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playEnd','result') AS game_results
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playEnd','score') AS user_score
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playEnd','opponentScore') AS opponent_score
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playEnd') AS event_details
       FROM victory.raw_events AS a
       WHERE 
        JSON_EXTRACT_PATH_TEXT(event_details, 'playEnd') IS NOT NULL
        AND event_id NOT IN (SELECT DISTINCT event_id FROM victory.play_end)
		""")

    
    
	play_start_insert = ("""
	INSERT INTO victory.play_start
       SELECT  user_id
       ,  session_id 
       , event_id
       , (TIMESTAMP 'epoch' + (event_timestamp /1000) * INTERVAL '1 second') AS event_timestamp 
       , 'PlayStart' AS event_name 
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playStart', 'matchId') AS match_id 
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playStart', 'court') AS court
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playStart', 'opponentId') AS opponent_id 
       , JSON_EXTRACT_PATH_TEXT(event_details, 'playStart') AS event_details
       FROM victory.raw_events AS a
       WHERE 
        JSON_EXTRACT_PATH_TEXT(event_details, 'playStart') IS NOT NULL
        AND event_id NOT IN (SELECT DISTINCT event_id FROM victory.play_start)
		""")
    
class CreateTables:
    
    create_table_statement = ("""
    CREATE TABLE IF NOT EXISTS victory.raw_events (
        user_id VARCHAR (255)
        , app_ver VARCHAR 
        , platform VARCHAR
        , country VARCHAR
        , event_timestamp BIGINT 
        , session_id VARCHAR
        , event_id VARCHAR
        , session_purchases INT
        , session_value INT
        , event_details VARCHAR (max)
    );

    CREATE TABLE IF NOT EXISTS victory.sessions (
        user_id VARCHAR (255)
    , session_id VARCHAR 
    ,  app_ver VARCHAR 
    , session_start TIMESTAMP 
    , session_end TIMESTAMP
    , country VARCHAR
    , session_purchases INT
    , session_value INT
    );


    CREATE TABLE IF NOT EXISTS public.kochava_installs (
    install_id INT
    , udid VARCHAR
    , udid_md5 VARCHAR
    , odin VARCHAR
    , bid_price FLOAT
    , matched_on VARCHAR
    , android_id VARCHAR
    , cpi_price FLOAT
    , imei_sha1 VARCHAR
    , campaign_id VARCHAR
    , device_ver VARCHAR
    , idfv VARCHAR
    , facebook_id VARCHAR
    , imei VARCHAR
    , request VARCHAR
    , click_id VARCHAR(512)
    , udid_sha1 VARCHAR
    , site_id VARCHAR(512)
    , imei_md5 VARCHAR
    , tracker_name VARCHAR
    , install_dtm VARCHAR
    , install_date DATE
    , click_date DATE
    , mac VARCHAR
    , campaign_name VARCHAR
    , network_id VARCHAR
    , user_id VARCHAR
    , country_code VARCHAR
    , creative VARCHAR
    , bid_id VARCHAR
    , android_sha1 VARCHAR
    , campaign_real_name VARCHAR
    , android_md5 VARCHAR
    , matched_by VARCHAR
    , kochava_device_id VARCHAR
    , idfa VARCHAR
    , network_name VARCHAR
    , idfv_sha1 VARCHAR
    , tracker_id VARCHAR
    , adid VARCHAR
    , app_id VARCHAR
    , app_name VARCHAR
    , postback VARCHAR
    , api_key VARCHAR
    , row_id INT
    , insert_time timestamp
    , attribution_creative VARCHAR
    , cp_name1 VARCHAR
    , cp_value1 VARCHAR
    , install_price DOUBLE PRECISION
    , fire_adid VARCHAR
    , cp_3 VARCHAR
    , "iad.iad-conversion-type" VARCHAR
    , cp_2 VARCHAR
    , googl_v2_campaign_name VARCHAR
    , load_time timestamp
    );


    CREATE TABLE IF NOT EXISTS public.singular_raw (
         activity_date DATE
         , app VARCHAR
         , os VARCHAR
         , source VARCHAR
         , campaign VARCHAR
         , country VARCHAR
         , custom_installs FLOAT
         , adn_cost FLOAT
         , adn_clicks FLOAT
         , adn_impressions FLOAT
         , adn_installs FLOAT
         , tracker_installs FLOAT
         , tracker_clicks FLOAT
         , alt_source VARCHAR
         , tracker_name VARCHAR
         , sub_campaign_name VARCHAR
         , sub_campaign_id VARCHAR
         , sub_adnetwork_name VARCHAR
         , campaign_id VARCHAR
         , keyword VARCHAR
         , load_time timestamp

    );

    CREATE TABLE IF NOT EXISTS victory.play_start (
    user_id VARCHAR
    ,  session_id VARCHAR 
    , event_id VARCHAR
    , event_timestamp TIMESTAMP
    , event_name VARCHAR
    , match_id VARCHAR
    , court VARCHAR
    , opponent_id VARCHAR
    , event_details VARCHAR (max)
    ); 
    
    CREATE TABLE IF NOT EXISTS victory.play_end (
    user_id VARCHAR
    ,  session_id VARCHAR 
    , event_id VARCHAR
    , event_timestamp TIMESTAMP
    , event_name VARCHAR
    , match_id VARCHAR
    , court VARCHAR
    , opponent_id VARCHAR
    , game_result VARCHAR
    , user_score VARCHAR
    , opponent_score VARCHAR
    , event_details VARCHAR (max)
    ); 
    """
     )
                              
class  DeleteData: 
     raw_table_delete = ("""
     DELETE FROM victory.raw_events
     WHERE 
     (TIMESTAMP 'epoch' + (event_timestamp /1000) * INTERVAL '1 second') - current_date >= 31
     """)
