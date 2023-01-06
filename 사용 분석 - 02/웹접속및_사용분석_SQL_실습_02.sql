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
DAU, WAU, MAU �� ���� ��� WAU ����
*************************************/
-- �Ϻ� �湮�� �� ��(DAU)
select date_trunc('day', visit_stime)::date as d_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
group by date_trunc('day', visit_stime)::date;

-- �ֺ� �湮�� ����(WAU)
select date_trunc('week', visit_stime)::date as week_day, count(distinct user_id) as user_cnt
from ga.ga_sess 
group by date_trunc('week', visit_stime)::date order by 1;

-- ���� �湮�� ����(MAU)
select date_trunc('month', visit_stime)::date as month_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
group by date_trunc('month', visit_stime)::date;

-- ���� ��� WAU ����
with temp_01 as (
	select date_trunc('week', visit_stime)::date as week_day
		, count(distinct user_id) as user_cnt
	from ga.ga_sess group by date_trunc('week', visit_stime)::date
)
select week_day, user_cnt
	-- ���� ���� �����Ͱ� ������ ���� user_cnt�� ������. 
	, coalesce(lag(user_cnt) over (order by week_day), user_cnt) as prev_user_cnt
	-- ���� ���� �����Ͱ� ������ 100
	, coalesce(round(100.0 * user_cnt/lag(user_cnt) over (order by week_day), 2), 100.0) as prev_pct
from temp_01;

/************************************
DAU�� MAU�� ����. stickiness ���� ����ڵ��� �󸶳� ���� �� �����ߴ°�? ��湮 ��ǥ�� ������ Ȱ��ȭ ��ǥ ����.  
*************************************/
 
with temp_dau as (
select to_char(date_trunc('day', visit_stime)::date, 'yyyymmdd') as d_day
	, count(distinct user_id) as dau
from ga.ga_sess 
where visit_stime between to_date('2016-08-01', 'yyyy-mm-dd') and to_date('2016-08-31', 'yyyy-mm-dd')
group by date_trunc('day', visit_stime)::date
), 
temp_mau as ( 
select to_char(date_trunc('month', visit_stime)::date, 'yyyymm') as month_day
	, count(distinct user_id) as mau 
from ga.ga_sess 
where visit_stime between to_char('2016-08-01', 'yyyy-mm-dd') and to_char('2016-08-31', 'yyyy-mm-dd')
group by date_trunc('month', visit_stime)::date
)
select a.d_day, a.dau, b.month_day, b.mau
	, round(100.0 * a.dau/b.mau, 2) as dau_mau_ratio
from temp_dau a join temp_mau b
on substring(a.d_day, 1, 6) = b.month_day

-- ��ü �Ⱓ�� �� ���� ���� ���� �湮�� ������ ��ȸ. 
select user_id, count(*)
from ga_sess group by user_id order by 2 desc;

-- Ư�� Ƚ�� �������� �湮�� �� �� - 8�� �Ѵް� 1ȸ, 2-3ȸ, 4-8, 9-14, 15-25, 26ȸ �̻� �湮�� �� �Ǽ� ��ȸ.
with temp_01 as (  
	select user_id, count(*) as cnt
	from ga_sess where visit_stime between to_date('2016-08-01', 'yyyy-mm-dd') and to_date('2016-08-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss') group by user_id 
)
select case when cnt = 1 then '0_only_first_session'
			  when cnt between 2 and 3 then '1_lt_3'
			  when cnt between 4 and 8 then '2_lt_8'
			  when cnt between 9 and 14 then '3_lt_14'
			  when cnt between 15 and 25 then '4_lt_25'
			  when cnt >= 26 then '5_over_26' end as gubun
	   , count(user_id) as user_cnt
from temp_01 
group by case when cnt = 1 then '0_only_first_session'
			  when cnt between 2 and 3 then '1_lt_3'
			  when cnt between 4 and 8 then '2_lt_8'
			  when cnt between 9 and 14 then '3_lt_14'
			  when cnt between 15 and 25 then '4_lt_25'
			  when cnt >= 26 then '5_over_26' end
order by 1;

-- ����ڴ� �ּ� 8���� �������� 3�� ���� �����Ǿ�� ��. 8�� �Ѵް� 1ȸ, 2-3ȸ, 4-8, 9-14, 15-25, 26ȸ �̻� �湮�� �� �Ǽ� ��ȸ
with temp_01 as (  
	select a.user_id, count(*) as cnt
	from ga_sess a 
		join ga_users b 
		on a.user_id = b.user_id 
	where visit_stime between to_date('2016-08-01', 'yyyy-mm-dd') and to_date('2016-08-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss') 
	and b.create_time <= to_date('2016-08-31', 'yyyy-mm-dd hh24:mi:ss') - 2
	group by a.user_id 
)
select case when cnt = 1 then '0_only_first_session'
			  when cnt between 2 and 3 then '1_lt_3'
			  when cnt between 4 and 8 then '2_lt_8'
			  when cnt between 9 and 14 then '3_lt_14'
			  when cnt between 15 and 25 then '4_lt_25'
			  when cnt >= 26 then '5_over_26' end as gubun
	   , count(user_id) as user_cnt
from temp_01 
group by case when cnt = 1 then '0_only_first_session'
			  when cnt between 2 and 3 then '1_lt_3'
			  when cnt between 4 and 8 then '2_lt_8'
			  when cnt between 9 and 14 then '3_lt_14'
			  when cnt between 15 and 25 then '4_lt_25'
			  when cnt >= 26 then '5_over_26' end
order by 1;


/* ���� Ư�� Ƚ�� �������� �湮�� �� �� ���ϱ� 
  �Ʒ� ���ܺ��� ����. 
1. ����ں� ���� ���� Ƚ��, ���� 3�� ���� ������ ����� ���� 
2.  ����ں� ���� ���� ������ Ƚ��, ���� 3�� ���� ������ ����� ����
3.  gubun ���� pivot �Ͽ� ����
*/

-- user �������ڰ� �ش� ���� ������ �Ͽ��� 3������ user ����. 
select user_id, create_time, (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date
from ga_users
where create_time <= (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date - 2;

-- ����ں� ���� ���� Ƚ��, ���� 3�� ���� ������ ����� ���� 
select a.user_id, date_trunc('month', visit_stime)::date as month, count(*) as monthly_user_cnt  
from ga_sess a 
	join ga_users b 
	on a.user_id = b.user_id 
where b.create_time <= (date_trunc('month', b.create_time) + interval '1 month' - interval '1 day')::date - 2
group by a.user_id, date_trunc('month', visit_stime)::date;

