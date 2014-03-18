weblogs = LOAD '/user/pdi/weblogs/parse/part*' USING PigStorage('\t')
        AS (
client_ip:chararray,
full_request_date:chararray,
day:int,
month:chararray,
month_num:int,
year:int,
hour:int,
minute:int,
second:int,
timezone:chararray,
http_verb:chararray,
uri:chararray,
http_status_code:chararray,
bytes_returned:chararray,
referrer:chararray,
user_agent:chararray
);

weblog_group = GROUP weblogs by (client_ip, year, month_num);
weblog_count = FOREACH weblog_group GENERATE group.client_ip, group.year, group.month_num,  COUNT_STAR(weblogs) as pageviews;

STORE weblog_count INTO '/user/pdi/weblogs/aggregate_pig';
