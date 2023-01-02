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
