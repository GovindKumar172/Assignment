
-- 1. SQL Task 

/*

Software Used: 
DB: Amazon Redshift 
Storage: Amazon S3 
Client:  DataGrip to connec to redshift server

*/

/*
Create Table with given column  
Made column ID as distribution key as many join or filter can happen on this
*/
create table event_details.event_stats
(
	id integer distkey,
	event_name varchar(225),
	people_count integer encode az64
)
diststyle key
sortkey(id);
alter table event_details.event_stats owner to awsuser;

/* Copy command to get data from S3 and load in redshift schema's table */
copy event_details.event_stats
from
's3://data-engineer/event_sample_data.csv'
credentials 'aws_access_key_id=XXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYY'
delimiter ',' dateformat 'auto' REMOVEQUOTES IGNOREHEADER 1;


/* Query to find 3 or more consecutive events and amount of people more than 100 (inclusive) */
select id, event_name,people_count,
dense_rank() over (order by people_count desc) as event_rank
from event_details.event_stats
where people_count >=100
order by 1,2;


