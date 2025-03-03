create table if not exists grades(
	id serial primary key,
	user_id varchar(40),
	oauth_consumer_key varchar(30),
	lis_result_sourcedid varchar(200),
	lis_outcome_service_url varchar(250),
	is_correct smallint,
	attempt_type varchar(10),
	created_at varchar(30)
	)