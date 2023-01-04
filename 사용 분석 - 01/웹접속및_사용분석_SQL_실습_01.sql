/************************************
�Ϻ� ���ǰǼ�, �Ϻ� �湮 �����(����), ����ں� ��� ���� ��
*************************************/
with temp_01 as 
(
	select to_char(date_trunc('day', visit_stime), 'yyyy-mm-dd') as d_day
		-- ga_sess ���̺��� sess_id�� unique�ϹǷ� count(sess_id)�� ����
		, count(distinct sess_id) as daily_sess_cnt
		, count(sess_id) as daily_sess_cnt_again
		, count(distinct user_id) as daily_user_cnt 
	from ga.ga_sess group by to_char(date_trunc('day', visit_stime), 'yyyy-mm-dd')
)
select * 
	, 1.0*daily_sess_cnt/daily_user_cnt as avg_user_sessions
	-- �Ʒ��� ���� ������ ������ ���� �� postgresql�� ������ ����ȯ ��. 1.0�� �����ְų� ��������� float type���� 
	--, daily_sess_cnt/daily_user_cnt
from temp_01;

/************************************
DAU, WAU, MAU ���ϱ�
*************************************/
/* �Ʒ��� �̹� ���� ���� �����Ͱ� ���� ��츦 �����ϰ� DAU, WAU, MAU�� ������ */

-- �Ϻ� �湮�� �� ��(DAU)
select date_trunc('day', visit_stime)::date as d_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
--where visit_stime between to_date('2016-10-25', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('day', visit_stime)::date;

-- �ֺ� �湮�� ����(WAU)
select date_trunc('week', visit_stime)::date as week_d��y, count(distinct user_id) as user_cnt
from ga.ga_sess
--where visit_stime between to_date('2016-10-24', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('week', visit_stime)::date order by 1;

-- ���� �湮�� ����(MAU)
select date_trunc('month', visit_stime)::date as month_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
--where visit_stime between to_date('2016-10-2', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('month', visit_stime)::date;

/* �Ʒ��� �Ϸ� �ֱ�� ��� DAU, WAU(���� 7��), MAU(���� 30��)�� ��� ����. */

-- interval�� ���� 7�� ���ϱ�
select to_date('20161101', 'yyyymmdd') - interval '7 days';

-- ���� ���� �������� �� 7���� WAU ���ϱ�
select :current_date, count(distinct user_id) as wau
from ga_sess
where visit_stime >= (:current_date - interval '7 days') and visit_stime < :current_date;

-- ���� ���� �������� ������ DAU ���ϱ�
select :current_date, count(distinct user_id) as dau
from ga_sess
where visit_stime >= (:current_date - interval '1 days') and visit_stime < :current_date;

-- ��¥���� DAU, WAU, MAU ���� ������ ���̺� ����. 
create table if not exists daily_acquisitions
(d_day date,
dau integer,
wau integer,
mau integer
);

--daily_acquisitions ���̺� ������ current_date�� DAU, WAU, MAU�� �Է�
insert into daily_acquisitions
select 
	:current_date, 
	-- scalar subquery�� select ���� ��밡���ϸ� �� �Ѱ�, �� �÷��� ����Ǿ�� ��. 
	(select count(distinct user_id) as dau
	from ga_sess
	where visit_stime >= (:current_date - interval '1 days') and visit_stime < :current_date
	),
	(select count(distinct user_id) as wau
	from ga_sess
	where visit_stime >= (:current_date - interval '7 days') and visit_stime < :current_date
	),
	(select count(distinct user_id) as mau
	from ga_sess
	where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
	)
;
-- ������ �Է� Ȯ��. 
select * from daily_acquisitions;


-- ���� ���ں��� DAU ����
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as dau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '1 days') and visit_stime < b.current_date
group by b.current_date;

-- ���� ���ں��� ���� 7�� WAU ����. 
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as wau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '7 days') and visit_stime < b.current_date
group by b.current_date;

-- ���� ���ں��� ���� 30���� MAU ����. 
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as mau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '30 days') and visit_stime < b.current_date
group by b.current_date;


--������ Ȯ�� 81587, 80693, 80082
select count(distinct user_id) as mau
	from ga_sess
	where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date;


-- ���� ���ں��� DAU �����ϴ� �ӽ� ���̺� ����
drop table if exists daily_dau;

create table daily_dau
as
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as dau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '1 days') and visit_stime < b.current_date
group by b.current_date
;

-- ���� ���ں��� WAU �����ϴ� �ӽ� ���̺� ����
drop table if exists daily_wau;

create table daily_wau
as
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as wau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '7 days') and visit_stime < b.current_date
group by b.current_date;

-- ���� ���ں��� MAU �����ϴ� �ӽ� ���̺� ����
drop table if exists daily_mau;

create table daily_mau
as
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as mau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '30 days') and visit_stime < b.current_date
group by b.current_date;

-- DAU, WAU, MAU �ӽ����̺��� ���ں��� �����Ͽ� daily_acquisitions ���̺� ����. 
drop table if exists daily_acquisitions;

create table daily_acquisitions
as
select a.current_date, a.dau, b.wau, c.mau
from daily_dau a
	join daily_wau b on a.current_date = b.current_date
	join daily_mau c on a.current_date = c.current_date
;

select * from daily_acquisitions;

drop table if exists daily_acquisitions;

-- �Ʒ��� ���� current_date �÷����� curr_date�� �����մϴ�. 
create table daily_acquisitions
as
select a.current_date as curr_date, a.dau, b.wau, c.mau
from daily_dau a
	join daily_wau b on a.current_date = b.current_date
	join daily_mau c on a.current_date = c.current_date
;

/************************************
DAU�� MAU�� ����. ������(stickiness)  ���� ����ڵ��� �󸶳� ���� ����ڰ� �ֱ������� �湮�ϴ°�? ��湮 ��ǥ�� ������ Ȱ��ȭ ��ǥ ����.
*************************************/
--DAU�� MAU�� ���� 
with 
temp_dau as (
select :current_date as curr_date, count(distinct user_id) as dau
from ga.ga_sess
where visit_stime >= (:current_date - interval '1 days') and visit_stime < :current_date
), 
temp_mau as (
select :current_date as curr_date, count(distinct user_id) as mau
from ga.ga_sess
where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
)
select a.current_day, a.dau, b.mau, round(100.0 * a.dau/b.mau, 2) as stickieness
from temp_dau a
	join temp_mau b on a.curr_date = b.curr_date
;

-- �����ϰ� stickiess, ��� stickness
select *, round(100.0 * dau/mau, 2) as stickieness
	, round(avg(100.0 * dau/mau) over(), 2) as avg_stickieness
from ga.daily_acquisitions
where curr_date between to_date('2016-10-25', 'yyyy-mm-dd') and to_date('2016-10-31', 'yyyy-mm-dd')



/************************************
����ں� ���� ���� ���� Ƚ�� ������ ���� ����
step 1: ����ں� ���� ���� Ƚ��, (���� 3�� ���� ������ ����� ����) 
step 2: ����ں� ���� ���� Ƚ�� ������ ���� . ���� + ���� Ƚ�� �������� Group by
step 3: gubun ���� pivot �Ͽ� ����
*************************************/
 
-- user �������ڰ� �ش� ���� ������ �Ͽ��� 3������ user ����. 
-- ���� ������ ���� ���ϱ�. 
-- postgresql�� last_day()�Լ��� ����. ������ �ش� ���ڰ� ���� ���� ù��° ��¥ ���� 10�� 5���̸� 10�� 1�Ͽ� 1���� ���ϰ� �ű⿡ 1���� ��
-- �� 10�� 5�� -> 10�� 1�� -> 11�� 1�� -> 10�� 31�� ������ �����.

select user_id, create_time, (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date
from ga.ga_users
where create_time <= (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date - 2;

-- ����ں� ���� �������� Ƚ��, ���� 3�� ���� ������ ����� ���� 
select a.user_id, date_trunc('month', visit_stime)::date as month
	-- ����ں� ���� �Ǽ�. ���� ���� �Ǽ��� �ƴϹǷ� count(distinct user_id)�� �������� ����. 
	, count(*) as monthly_user_cnt  
from ga_sess a 
	join ga_users b on a.user_id = b.user_id 
where b.create_time <= (date_trunc('month', b.create_time) + interval '1 month' - interval '1 day')::date - 2
group by a.user_id, date_trunc('month', visit_stime)::date;

-- ����ں� ���� ���� ���� Ƚ�� ������ ����, ���� 3�� ���� ������ ����� ���� 
with temp_01 as (
	select a.user_id, date_trunc('month', visit_stime)::date as month, count(*) as monthly_user_cnt  
	from ga.ga_sess a 
		join ga_users b on a.user_id = b.user_id 
	where b.create_time <= (date_trunc('month', b.create_time) + interval '1 month' - interval '1 day')::date - 2
	group by a.user_id, date_trunc('month', visit_stime)::date 
)
select month
	,case when monthly_user_cnt = 1 then '0_only_first_session'
		  when monthly_user_cnt between 2 and 3 then '2_between_3'
		  when monthly_user_cnt between 4 and 8 then '4_between_8'
		  when monthly_user_cnt between 9 and 14 then '9_between_14'
		  when monthly_user_cnt between 15 and 25 then '15_between_25'
		  when monthly_user_cnt >= 26 then 'over_26' end as gubun
	, count(*) as user_cnt 
from temp_01 
group by month, 
		 case when monthly_user_cnt = 1 then '0_only_first_session'
		  when monthly_user_cnt between 2 and 3 then '2_between_3'
		  when monthly_user_cnt between 4 and 8 then '4_between_8'
		  when monthly_user_cnt between 9 and 14 then '9_between_14'
		  when monthly_user_cnt between 15 and 25 then '15_between_25'
		  when monthly_user_cnt >= 26 then 'over_26' end
order by 1, 2;

-- gubun ���� pivot �Ͽ� ���� 
with temp_01 as (
	select a.user_id, date_trunc('month', visit_stime)::date as month, count(*) as monthly_user_cnt  
	from ga.ga_sess a 
		join ga.ga_users b 
		on a.user_id = b.user_id 
	where b.create_time <= (date_trunc('month', b.create_time) + interval '1 month' - interval '1 day')::date - 2
	group by a.user_id, date_trunc('month', visit_stime)::date 
), 
temp_02 as ( 
	select month
		,case when monthly_user_cnt = 1 then '0_only_first_session'
		      when monthly_user_cnt between 2 and 3 then '2_between_3'
		      when monthly_user_cnt between 4 and 8 then '4_between_8'
		      when monthly_user_cnt between 9 and 14 then '9_between_14'
		      when monthly_user_cnt between 15 and 25 then '15_between_25'
		      when monthly_user_cnt >= 26 then 'over_26' end as gubun
		, count(*) as user_cnt 
	from temp_01 
	group by month, 
			 case when monthly_user_cnt = 1 then '0_only_first_session'
			      when monthly_user_cnt between 2 and 3 then '2_between_3'
			      when monthly_user_cnt between 4 and 8 then '4_between_8'
			      when monthly_user_cnt between 9 and 14 then '9_between_14'
			      when monthly_user_cnt between 15 and 25 then '15_between_25'
			      when monthly_user_cnt >= 26 then 'over_26' end
)
select month, 
	sum(case when gubun='0_only_first_session' then user_cnt else 0 end) as "0_only_first_session"
	,sum(case when gubun='2_between_3' then user_cnt else 0 end) as "2_between_3"
	,sum(case when gubun='4_between_8' then user_cnt else 0 end) as "4_between_8"
	,sum(case when gubun='9_between_14' then user_cnt else 0 end) as "9_between_14"
	,sum(case when gubun='15_between_25' then user_cnt else 0 end) as "15_between_25"
	,sum(case when gubun='over_26' then user_cnt else 0 end) as "over_26"
from temp_02 
group by month order by 1;

/************************************
�Ѵ� �Ⱓ�߿� �ְ� �湮 Ƚ���� ����� �Ǽ�
*************************************/

with
temp_01 as (
select user_id,
	case when visit_date between '20160801' and '20160807' then '1st'
		 when visit_date between '20160808' and '20160814' then '2nd'
		 when visit_date between '20160815' and '20160821' then '3rd'
		 when visit_date between '20160822' and '20160828' then '4th'
		 when visit_date between '20160829' and '20160904' then '5th' end as week_gubun
	, count(distinct visit_date) as daily_visit_cnt
from ga_sess
where visit_date between '20160801' and '20160831'
group by user_id
, case when visit_date between '20160801' and '20160807' then '1st'
		 when visit_date between '20160808' and '20160814' then '2nd'
		 when visit_date between '20160815' and '20160821' then '3rd'
		 when visit_date between '20160822' and '20160828' then '4th'
		 when visit_date between '20160829' and '20160904' then '5th' end
)
select daily_visit_cnt
	, sum(case when week_gubun='1st' then 1 else 0 end) as week_1st_user_cnt
	, sum(case when week_gubun='2nd' then 1 else 0 end) as week_2nd_user_cnt
	, sum(case when week_gubun='3rd' then 1 else 0 end) as week_3rd_user_cnt
	, sum(case when week_gubun='4th' then 1 else 0 end) as week_4th_user_cnt
	, sum(case when week_gubun='5th' then 1 else 0 end) as week_5th_user_cnt
from temp_01 group by daily_visit_cnt
order by 1;

-- �ӽ� ���̺��� �̿��Ͽ� �������� �ְ� �Ⱓ ���� - �Ѵ� �Ⱓ�߿� �ְ� �湮 Ƚ���� ����� �Ǽ�
with
temp_00(week_gubun, start_date, end_date) as
(
values
('1st', '20160801', '20160807')
,('2nd', '20160808', '20160814')
,('3rd', '20160815', '20160821')
,('4th', '20160822', '20160828')
,('5th', '20160829', '20160904')
),
temp_01 as
(
select  a.user_id, b.week_gubun
	, count(distinct visit_date) as daily_visit_cnt
from ga_sess a
	join temp_00 b on a.visit_date between b.start_date and end_date
 --where a.visit_date between (select min(start_date) from temp_00) and (select max(end_date) from temp_00) -- ������ ���ؼ�
group by a.user_id, b.week_gubun
)
select daily_visit_cnt
	, sum(case when week_gubun='1st' then 1 else 0 end) as week_1st_user_cnt
	, sum(case when week_gubun='2nd' then 1 else 0 end) as week_2nd_user_cnt
	, sum(case when week_gubun='3rd' then 1 else 0 end) as week_3rd_user_cnt
	, sum(case when week_gubun='4th' then 1 else 0 end) as week_4th_user_cnt
	, sum(case when week_gubun='5th' then 1 else 0 end) as week_5th_user_cnt
from temp_01 group by daily_visit_cnt
order by 1;


/************************************
����ڰ� ù ���� ���� �� �ι�° ���� ���ӱ��� �ɸ��� ���, �ִ�, �ּ�, 4���� percentile �ð� ����
step 1: ����� ���� ���� �ð��� ���� session �� ���� �ű�.
step 2: session �� ������ ù��°�� �ι�° �ΰ� ����
step 3: ����� ���� ù��° ������ ���� ���� �ι�° ������ ���� �ð� ���̸� ���� ����
step 4: step 3�� �����͸� ��ü ���, �ִ�, �ּ�, 4���� percentile �ð� ���ϱ�
*************************************/

-- ����� ���� ���� �ð��� ���� session �� ���� �ű�.
select user_id, row_number() over (partition by user_id order by visit_stime) as session_rnum
	, visit_stime
	-- ���Ŀ� 1�� session�� �ִ� ����ڴ� �����ϱ� ���� ���.
	, count(*) over (partition by user_id) as session_cnt
from ga_sess order by user_id, session_rnum;

--session �� ������ ù��°�� �ι�° �ΰ� �����ϰ� ����� ���� ù��° ������ ���� ���� �ι�° ������ ���� �ð� ���̸� ���� ����
with
temp_01 as (
	select user_id, row_number() over (partition by user_id order by visit_stime) as session_rnum
	, visit_stime
	-- ���Ŀ� 1�� session�� �ִ� ����ڴ� �����ϱ� ���� ���.
	, count(*) over (partition by user_id) as session_cnt
from ga_sess
)
select user_id
	-- ����ں��� ù��° ����, �ι�° ���Ǹ� �����Ƿ� max(visit_stime)�� �ι�° ���� ���� �ð�, min(visit_stime)�� ù��° ���� ���� �ð�.
	, max(visit_stime) - min(visit_stime) as sess_time_diff
from temp_01 where session_rnum <= 2 and session_cnt > 1 -- ù��° �ι�° ���Ǹ� �������� ù��° ���Ӹ� �ִ� ����ڸ� �����ϱ�
group by user_id;

-- step 3�� �����͸� ��ü ���, �ִ밪, �ּҰ�, 4���� percentile  ���ϱ�.
with
temp_01 as (
	select user_id, row_number() over (partition by user_id order by visit_stime) as session_rnum
		, visit_stime
		-- ���Ŀ� 1�� session�� �ִ� ����ڴ� �����ϱ� ���� ���.
		, count(*) over (partition by user_id) as session_cnt
	from ga_sess
),
temp_02 as (
	select user_id
		-- ����ں��� ù��° ����, �ι�° ���Ǹ� �����Ƿ� max(visit_stime)�� �ι�° ���� ���� �ð�, min(visit_stime)�� ù��° ���� ���� �ð�.
		, max(visit_stime) - min(visit_stime) as sess_time_diff
	from temp_01 where session_rnum <= 2 and session_cnt > 1
	group by user_id
)
-- postgresql avg(time)�� interval�� ����� ������� ����. justify_inteval()�� �����ؾ� ��.
select justify_interval(avg(sess_time_diff)) as avg_time
    , max(sess_time_diff) as max_time, min(sess_time_diff) as min_time
	, percentile_disc(0.25) within group (order by sess_time_diff) as percentile_1
	, percentile_disc(0.5) within group (order by sess_time_diff)	as percentile_2
	, percentile_disc(0.75) within group (order by sess_time_diff)	as percentile_3
	, percentile_disc(1.0) within group (order by sess_time_diff)	as percentile_4
from temp_02
where sess_time_diff::interval > interval '0 second';


/************************************
MAU�� �ű� �����, ���� �����(�� �湮) �Ǽ��� �и��Ͽ� ����(���� �Ǽ��� �Բ� ����)
*************************************/

with
temp_01 as (
select a.sess_id, a.user_id, a.visit_stime, b.create_time
	, case when b.create_time >= (:current_date - interval '30 days') and b.create_time < :current_date then 1
	     else 0 end as is_new_user
from ga.ga_sess a
	join ga.ga_users b on a.user_id = b.user_id
where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
)
select count(distinct user_id) as user_cnt
	, count(distinct case when is_new_user = 1 then user_id end) as new_user_cnt
	, count(distinct case when is_new_user = 0 then user_id end) as repeat_user_cnt
	, count(*) as sess_cnt
from temp_01;



/************************************
ä�κ��� MAU�� �ű� �����, ���� ����ڷ� ������, ä�κ� �������� �Բ� ���.
*************************************/
select channel_grouping, count(distinct user_id) from ga.ga_sess group by channel_grouping;

with
temp_01 as (
select a.sess_id, a.user_id, a.visit_stime, b.create_time, channel_grouping
	, case when b.create_time >= (:current_date - interval '30 days') and b.create_time < :current_date then 1
	     else 0 end as is_new_user
from ga.ga_sess a
	join ga.ga_users b on a.user_id = b.user_id
where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
),
temp_02 as (
select channel_grouping
	, count(distinct case when is_new_user = 1 then user_id end) as new_user_cnt
	, count(distinct case when is_new_user = 0 then user_id end) as repeat_user_cnt
	, count(distinct user_id) as channel_user_cnt
	, count(*) as sess_cnt
from temp_01
group by channel_grouping
)
select channel_grouping, new_user_cnt, repeat_user_cnt, channel_user_cnt, sess_cnt
	, 100.0*new_user_cnt/sum(new_user_cnt) over () as new_user_cnt_by_channel
	, 100.0*repeat_user_cnt/sum(repeat_user_cnt) over () as repeat_user_cnt_by_channel
from temp_02;


/************************************
ä�κ� ���� ����� �Ǽ��� ����ݾ� �� ����, �ֹ� ����� �Ǽ��� �ֹ� ���� �ݾ� �� ����
ä�κ��� ���� ����� �Ǽ��� ���� �ݾ��� ���ϰ� ���� ����� �Ǽ� ��� ���� �ݾ� ������ ����.
���� ���� ����� �߿��� �ֹ��� ������ ����� �Ǽ��� ���� �� �ֹ� ����� �Ǽ� ��� ���� �ݾ� ������ ����
*************************************/
with temp_01 as (
	select a.sess_id, a.user_id, a.channel_grouping
		, b.order_id, b.order_time, c.product_id, c.prod_revenue
	from ga_sess a
		left join orders b on a.sess_id = b.sess_id
		left join order_items c on b.order_id = c.order_id
	where a.visit_stime >= (:current_date - interval '30 days') and a.visit_stime < :current_date
)
select channel_grouping
	, sum(prod_revenue) as ch_amt -- ä�κ� ����
	--, count(distinct sess_id) as ch_sess_cnt -- ä�κ� ���� ���� ��
	, count(distinct user_id) as ch_user_cnt -- ä�κ� ���� ����� ��
	--, count(distinct case when order_id is not null then sess_id end) as ch_ord_sess_cnt -- ä�κ� �ֹ� ���� ���Ǽ�
	, count(distinct case when order_id is not null then user_id end) as ch_ord_user_cnt -- ä�κ� �ֹ� ���� ����ڼ�
	--, sum(prod_revenue)/count(distinct sess_id) as ch_amt_per_sess -- ���� ���Ǻ� �ֹ� ���� �ݾ�
	, sum(prod_revenue)/count(distinct user_id) as ch_amt_per_user -- ���� ���� ����ں� �ֹ� ���� �ݾ�
	-- �ֹ� ���Ǻ� ���� �ݾ�
	--, sum(prod_revenue)/count(distinct case when order_id is not null then sess_id end) as ch_ord_amt_per_sess
	-- �ֹ� ���� ����ں� ���� �ݾ�
	, sum(prod_revenue)/count(distinct case when order_id is not null then user_id end) as ch_ord_amt_per_user
from temp_01
group by channel_grouping order by ch_user_cnt desc;


/************************************
device �� ���� �Ǽ� , ��ü �Ǽ���� device�� ���� �Ǽ�
��/�ֺ� device�� ���ӰǼ�
*************************************/

-- device �� ���� �Ǽ�
select device_category, count(*) as device_cnt
from ga_sess group by device_category;

-- ��ü �Ǽ� ��� device�� ���� �Ǽ�
with temp_01 as (
select count(*) as total_cnt from ga_sess
),
temp_02 as (
select device_category, count(*) as device_cnt
from ga_sess group by device_category
)
select device_category, device_cnt, 1.0*device_cnt/total_cnt
from temp_01, temp_02;

-- mobile�� tablet�� �Բ� ���ļ� mobile_tablet���� ���� �Ǽ� ����
select
	case when device_category in ('mobile', 'tablet') then 'mobile_tablet'
			  when device_category = 'desktop' then 'desktop' end as device_category
	, count(*) as device_cnt
from ga_sess
group by case when device_category in ('mobile', 'tablet') then 'mobile_tablet'
			  when device_category = 'desktop' then 'desktop' end;


-- �Ϻ� �����ڸ� desktop, mobile, tablet �� ���� �����ڼ� ���.
select date_trunc('day', visit_stime)
	, sum(case when device_category = 'desktop' then 1 else 0 end) as desktop_cnt
	, sum(case when device_category = 'mobile' then 1 else 0 end) as mobile_cnt
	, sum(case when device_category = 'tablet' then 1 else 0 end) as tablet_cnt
	, count(*)
from ga_sess
group by date_trunc('day', visit_stime);

-- �ֺ� �����ڸ� desktop, mobile, tablet �� ���� �����ڼ� ���.
select date_trunc('week', visit_stime)
	, sum(case when device_category = 'desktop' then 1 else 0 end) as desktop_cnt
	, sum(case when device_category = 'mobile' then 1 else 0 end) as mobile_cnt
	, sum(case when device_category = 'tablet' then 1 else 0 end) as tablet_cnt
	, count(*)
from ga_sess
group by date_trunc('week', visit_stime);


-- ���� device �� ����� device�� ���Ǵ� ����� ����ں� ����� ����.
with temp_01 as (
	select a.order_id, a.order_time,  b.product_id, b.prod_revenue, c.sess_id, c.user_id, c.device_category
	from orders a
		join order_items b
			on a.order_id = b.order_id
		join ga_sess c
			on a.sess_id = c.sess_id
	where a.order_status = 'delivered'
)
select device_category, sum(prod_revenue) device_sum_amount
	, count(distinct sess_id) as sess_cnt
	, count(distinct user_id) as user_cnt
	, sum(prod_revenue)/count(distinct sess_id) as sum_amount_per_sess
	, sum(prod_revenue)/count(distinct user_id) as sum_amount_per_user
from temp_01;
group by device_category;