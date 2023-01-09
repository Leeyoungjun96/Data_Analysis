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

-- ����ں� ���� ���� ������ Ƚ��, ���� 3�� ���� ������ ����� ���� 
with temp_01 as (
	select a.user_id, date_trunc('month', visit_stime)::date as month, count(*) as monthly_user_cnt  
	from ga_sess a 
		join ga_users b 
		on a.user_id = b.user_id 
	where b.create_time <= (date_trunc('month', b.create_time) + interval '1 month' - interval '1 day')::date - 2
	group by a.user_id, date_trunc('month', visit_stime)::date 
)
select month
	,case when monthly_user_cnt = 1 then '0_only_first_session'
		  when monthly_user_cnt between 2 and 3 then '1_lt_3'
		  when monthly_user_cnt between 4 and 8 then '2_lt_8'
		  when monthly_user_cnt between 9 and 14 then '3_lt_14'
		  when monthly_user_cnt between 15 and 25 then '4_lt_25'
		  when monthly_user_cnt >= 26 then '5_over_26' end as gubun
	, count(*) as user_cnt 
from temp_01 
group by month, 
		 case when monthly_user_cnt = 1 then '0_only_first_session'
			  when monthly_user_cnt between 2 and 3 then '1_lt_3'
			  when monthly_user_cnt between 4 and 8 then '2_lt_8'
			  when monthly_user_cnt between 9 and 14 then '3_lt_14'
			  when monthly_user_cnt between 15 and 25 then '4_lt_25'
			  when monthly_user_cnt >= 26 then '5_over_26' end
order by 1, 2;

-- gubun ���� pivot �Ͽ� ���� 
with temp_01 as (
	select a.user_id, date_trunc('month', visit_stime)::date as month, count(*) as monthly_user_cnt  
	from ga_sess a 
		join ga_users b 
		on a.user_id = b.user_id 
	where b.create_time <= (date_trunc('month', b.create_time) + interval '1 month' - interval '1 day')::date - 2
	group by a.user_id, date_trunc('month', visit_stime)::date 
), 
temp_02 as ( 
	select month
		,case when monthly_user_cnt = 1 then '0_only_first_session'
			  when monthly_user_cnt between 2 and 3 then '1_lt_3'
			  when monthly_user_cnt between 4 and 8 then '2_lt_8'
			  when monthly_user_cnt between 9 and 14 then '3_lt_14'
			  when monthly_user_cnt between 15 and 25 then '4_lt_25'
			  when monthly_user_cnt >= 26 then '5_over_26' end as gubun
		, count(*) as user_cnt 
	from temp_01 
	group by month, 
			 case when monthly_user_cnt = 1 then '0_only_first_session'
				  when monthly_user_cnt between 2 and 3 then '1_lt_3'
				  when monthly_user_cnt between 4 and 8 then '2_lt_8'
				  when monthly_user_cnt between 9 and 14 then '3_lt_14'
				  when monthly_user_cnt between 15 and 25 then '4_lt_25'
				  when monthly_user_cnt >= 26 then '5_over_26' end
)
select month, 
	sum(case when gubun='0_only_first_session' then user_cnt else 0 end) as "0_only_first_session"
	,sum(case when gubun='1_lt_3' then user_cnt else 0 end) as "1_lt_3"
	,sum(case when gubun='2_lt_8' then user_cnt else 0 end) as "2_lt_8"
	,sum(case when gubun='3_lt_14' then user_cnt else 0 end) as "3_lt_14"
	,sum(case when gubun='4_lt_25' then user_cnt else 0 end) as "4_lt_25"
	,sum(case when gubun='5_over_26' then user_cnt else 0 end) as "5_over_26"
from temp_02 
group by month order by 1;

/* ����ڰ� ù ���� �� �ι�° ���ӱ��� �ɸ��� ���, �ִ� �ð� ���� 
step 1: ����� ���� ���� �ð��� ���� session �� ���� �ű�. 
step 2: session �� ������ ù��°�� �ι�° �ΰ� ����
step 3: ����� ���� ù��° ������ ���� ���� �ι�° ������ ���� �ð� ���̸� ���� ����
step 4: step 3�� �����͸� ��ü ���/�ִ밪 ���ϱ� 
*/
-- ����� ���� ���� �ð��� ���� session �� ���� �ű�.
select user_id, row_number() over (partition by user_id order by visit_stime) session_seq 
	, visit_stime
from ga_sess order by user_id;

--session �� ������ ù��°�� �ι�° �ΰ� �����ϰ� ����� ���� ù��° ������ ���� ���� �ι�° ������ ���� �ð� ���̸� ���� ����
with
temp_01 as (
	select user_id, row_number() over (partition by user_id order by visit_stime) session_seq
		--, count(*) over (partition by user_id) --session_cnt ���� ù��° ���Ӹ� �ִ� ����ڸ� �����ϱ⸦ ���Ѵٸ� 
		, visit_stime
	from ga_sess
)
select user_id, max(visit_stime) - min(visit_stime) as sess_time_diff
from temp_01 where session_seq <= 2 --and session_cnt > 1 --session_cnt ���� ù��° ���Ӹ� �ִ� ����ڸ� �����ϱ⸦ ���Ѵٸ� 
group by user_id;

-- step 3�� �����͸� ��ü ���/�ִ밪 ���ϱ�. �̶� ����ڰ� ���� ���Ӹ� �ִ� ���� ����. 
with
temp_01 as (
	select user_id, row_number() over (partition by user_id order by visit_stime) session_seq 
		, visit_stime
	from ga_sess
), 
temp_02 as (
	select user_id, max(visit_stime) - min(visit_stime) as sess_time_diff
	from temp_01 where session_seq <= 2 
	group by user_id
)
select avg(sess_time_diff), max(sess_time_diff), min(sess_time_diff) 
from temp_02
where sess_time_diff::interval > interval '0 second';

/* �Ʒ��� ���� lead()�� �̿��Ͽ� ���� ���� ����. ����� �տ��� �ٸ�. ���� �ľ� �ʿ� */
with
temp_01 as (
	select user_id, row_number() over (partition by user_id order by visit_stime) session_seq 
		, visit_stime
	from ga_sess
), 
temp_02 as (
	select  user_id, session_seq, visit_stime 
		, lead(visit_stime, 1) over(partition by user_id order by visit_stime) as visit_stime_2nd
		from temp_01
)
select avg(time_diff_1to2)
from (
	select user_id, session_seq, visit_stime
		, visit_stime_2nd - visit_stime as time_diff_1to2
	from temp_02 where session_seq = 1 -- session_seq�� 1�� �����Ϳ� time_diff_xxx�� 1->2, 2->3, 3-> 4 ���ǰ��� �ð����̰� �� ����. 
) a;

-- ù ���� �� �ι�° ���ӱ��� �ɸ��� �ð��� 4������ ǥ��
with
temp_01 as (
	select user_id, row_number() over (partition by user_id order by visit_stime) session_seq 
		, visit_stime
	from ga_sess
), 
temp_02 as (
	select user_id, max(visit_stime) - min(visit_stime) as sess_time_diff
	from temp_01 where session_seq <= 2 
	group by user_id
)
select percentile_disc(0.25) within group (order by sess_time_diff) as quantile_1
	, percentile_disc(0.5) within group (order by sess_time_diff)	as quantile_2
	, percentile_disc(0.75) within group (order by sess_time_diff)	as quantile_3
	, percentile_disc(1.0) within group (order by sess_time_diff)	as quantile_4
from temp_02
where sess_time_diff::interval > interval '0 second';


/* ����ڰ� ù��° �������ӿ��� �ι�° ���� ���ӱ��� �ɸ��� ��� �ð��ܿ�, �ι�°->����°, ����°->�׹�° ���� �ɸ��� ��� �ð� ���
   �Ʒ� sql���� 1->2 ������ ��� ���� �ð��� ������ ���� SQL�� �ð��� �޶���.Ȯ�� ���. 
   step 1: ����ں��� lead()�� �̿��Ͽ� ���� ����, �ٴ��� ����, �ٴٴ��� ���� �ð��� ������. 
 */
with
temp_01 as (
	select user_id, row_number() over (partition by user_id order by visit_stime) session_seq 
		, visit_stime
	from ga_sess
)
select  user_id, session_seq, visit_stime 
	, lead(visit_stime, 1) over(partition by user_id order by visit_stime) as visit_stime_2nd
	, lead(visit_stime, 2) over(partition by user_id order by visit_stime) as visit_stime_3rd
	, lead(visit_stime, 3) over(partition by user_id order by visit_stime) as visit_stime_4th
from temp_01 order by user_id, session_seq;

with
temp_01 as (
	select user_id, row_number() over (partition by user_id order by visit_stime) session_seq 
		, visit_stime
	from ga_sess
), 
temp_02 as (
	select  user_id, session_seq, visit_stime 
		, lead(visit_stime, 1) over(partition by user_id order by visit_stime) as visit_stime_2nd
		, lead(visit_stime, 2) over(partition by user_id order by visit_stime) as visit_stime_3rd
		, lead(visit_stime, 3) over(partition by user_id order by visit_stime) as visit_stime_4th
	from temp_01
)
select avg(time_diff_1to2), avg(time_diff_2to3), avg(time_diff_3to4)
from (
	select user_id, session_seq, visit_stime
		, visit_stime_2nd - visit_stime as time_diff_1to2
		, visit_stime_3rd - visit_stime_2nd as time_diff_2to3
		, visit_stime_4th - visit_stime_3rd as time_diff_3to4
	from temp_02 where session_seq = 1 -- session_seq�� 1�� �����Ϳ� time_diff_xxx�� 1->2, 2->3, 3-> 4 ���ǰ��� �ð����̰� �� ����. 
) a;

with
temp_01 as (
	select user_id, row_number() over (partition by user_id order by visit_stime) session_seq 
		, visit_stime
	from ga_sess
), 
temp_02 as (
	select  user_id, session_seq, visit_stime 
		, lead(visit_stime, 1) over(partition by user_id order by visit_stime) as visit_stime_2nd
		, lead(visit_stime, 2) over(partition by user_id order by visit_stime) as visit_stime_3rd
		, lead(visit_stime, 3) over(partition by user_id order by visit_stime) as visit_stime_4th
	from temp_01
)
select user_id, session_seq, visit_stime
	, visit_stime_2nd - visit_stime as time_diff_1to2
	, visit_stime_3rd - visit_stime_2nd as time_diff_2to3
	, visit_stime_4th - visit_stime_3rd as time_diff_3to4
from temp_02;

/* device�� ���� �Ǽ� */

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

-- ����� ���� ��¥ �� �����ϰ� ����(Retention) ���� Ƚ��
with temp_01 as (
	select a.user_id, date_trunc('day', a.create_time) as user_create_date,  date_trunc('day', b.visit_stime) as sess_visit_date
		, count(*) cnt
	from ga_users a
		join ga_sess b
			on a.user_id = b.user_id
	group by a.user_id, date_trunc('day', a.create_time), date_trunc('day', b.visit_stime)
)
select user_create_date, count(*) as daily_create_visit_cnt
	, sum(case when sess_visit_date = user_create_date + interval '1 day' then 1 else 0 end ) as d1_cnt
	, sum(case when sess_visit_date = user_create_date + interval '2 day' then 1 else 0 end) as d2_cnt
	, sum(case when sess_visit_date = user_create_date + interval '3 day' then 1 else 0 end) as d3_cnt
	, sum(case when sess_visit_date = user_create_date + interval '4 day' then 1 else 0 end) as d4_cnt
	, sum(case when sess_visit_date = user_create_date + interval '5 day' then 1 else 0 end) as d5_cnt
	, sum(case when sess_visit_date = user_create_date + interval '6 day' then 1 else 0 end) as d6_cnt
	, sum(case when sess_visit_date = user_create_date + interval '7 day' then 1 else 0 end) as d7_cnt
from temp_01 
group by user_create_date order by 1;

-- ����� ���� ��¥ �� Device�� �����ϰ� ����(Retention) ���� Ƚ��
with temp_01 as (
	select a.user_id, device_category
		, date_trunc('day', a.create_time) as user_create_date,  date_trunc('day', b.visit_stime) as sess_visit_date
		, count(*) cnt
	from ga_users a
		join ga_sess b
			on a.user_id = b.user_id
	group by a.user_id, device_category, date_trunc('day', a.create_time), date_trunc('day', b.visit_stime)
)
select user_create_date, device_category, count(*) as daily_create_visit_cnt
	, sum(case when sess_visit_date = user_create_date + interval '1 day' then 1 else 0 end ) as d1_cnt
	, sum(case when sess_visit_date = user_create_date + interval '2 day' then 1 else 0 end) as d2_cnt
	, sum(case when sess_visit_date = user_create_date + interval '3 day' then 1 else 0 end) as d3_cnt
	, sum(case when sess_visit_date = user_create_date + interval '4 day' then 1 else 0 end) as d4_cnt
	, sum(case when sess_visit_date = user_create_date + interval '5 day' then 1 else 0 end) as d5_cnt
	, sum(case when sess_visit_date = user_create_date + interval '6 day' then 1 else 0 end) as d6_cnt
	, sum(case when sess_visit_date = user_create_date + interval '7 day' then 1 else 0 end) as d7_cnt
from temp_01 
group by user_create_date, device_category order by 1, 2;

--����� ���� ��¥ �� �����ϰ� ������(Retention ratio)
with temp_01 as (
	select a.user_id, date_trunc('day', a.create_time) as user_create_date,  date_trunc('day', b.visit_stime) as sess_visit_date
		, count(*) cnt
	from ga_users a
		join ga_sess b
			on a.user_id = b.user_id
	group by a.user_id, date_trunc('day', a.create_time), date_trunc('day', b.visit_stime)
)
select user_create_date, count(*) as daily_create_visit_cnt
	, 1.0*sum(case when sess_visit_date = user_create_date + interval '1 day' then 1 else 0 end)/count(*) as d1_retention_ratio
	, 1.0*sum(case when sess_visit_date = user_create_date + interval '2 day' then 1 else 0 end)/count(*) as d2_retention_ratio
	, 1.0*sum(case when sess_visit_date = user_create_date + interval '3 day' then 1 else 0 end)/count(*) as d3_retention_ratio
	, 1.0*sum(case when sess_visit_date = user_create_date + interval '4 day' then 1 else 0 end)/count(*) as d4_retention_ratio
	, 1.0*sum(case when sess_visit_date = user_create_date + interval '5 day' then 1 else 0 end)/count(*) as d5_retention_ratio
	, 1.0*sum(case when sess_visit_date = user_create_date + interval '6 day' then 1 else 0 end)/count(*) as d6_retention_ratio
	, 1.0*sum(case when sess_visit_date = user_create_date + interval '7 day' then 1 else 0 end)/count(*) as d7_retention_ratio
from temp_01 
group by user_create_date order by 1;

--����� ���� ��¥ �� Device�� �����ϰ� ������(Retention ratio)
with temp_01 as (
	select a.user_id, device_category, date_trunc('day', a.create_time) as user_create_date,  date_trunc('day', b.visit_stime) as sess_visit_date
		, count(*) cnt
	from ga_users a
		join ga_sess b
			on a.user_id = b.user_id
	group by a.user_id, device_category, date_trunc('day', a.create_time), date_trunc('day', b.visit_stime)
)
select user_create_date, device_category, count(*) as daily_create_visit_cnt
	, sum(case when sess_visit_date = user_create_date + interval '1 day' then 1 else 0 end)/count(*) as d1_retention_ratio
	, sum(case when sess_visit_date = user_create_date + interval '2 day' then 1 else 0 end)/count(*) as d2_retention_ratio
	, sum(case when sess_visit_date = user_create_date + interval '3 day' then 1 else 0 end)/count(*) as d3_retention_ratio
	, sum(case when sess_visit_date = user_create_date + interval '4 day' then 1 else 0 end)/count(*) as d4_retention_ratio
	, sum(case when sess_visit_date = user_create_date + interval '5 day' then 1 else 0 end)/count(*) as d5_retention_ratio
	, sum(case when sess_visit_date = user_create_date + interval '6 day' then 1 else 0 end)/count(*) as d6_retention_ratio
	, sum(case when sess_visit_date = user_create_date + interval '7 day' then 1 else 0 end)/count(*) as d7_retention_ratio
from temp_01 
group by user_create_date, device_category order by 1, 2;

-- ���� ������ �缱���� ǥ���ϱ�. 
with temp_01 as (
	select a.user_id, date_trunc('day', a.create_time) as user_create_date,  date_trunc('day', b.visit_stime) as sess_visit_date
		, count(*) cnt
	from ga_users a
		join ga_sess b
			on a.user_id = b.user_id
	group by a.user_id, date_trunc('day', a.create_time), date_trunc('day', b.visit_stime)
),
temp_02 as ( 
	select user_create_date, count(*) as daily_create_visit_cnt
		, sum(case when sess_visit_date = user_create_date + interval '1 day' then 1 else 0 end ) as d1_cnt
		, sum(case when sess_visit_date = user_create_date + interval '2 day' then 1 else 0 end) as d2_cnt
		, sum(case when sess_visit_date = user_create_date + interval '3 day' then 1 else 0 end) as d3_cnt
		, sum(case when sess_visit_date = user_create_date + interval '4 day' then 1 else 0 end) as d4_cnt
		, sum(case when sess_visit_date = user_create_date + interval '5 day' then 1 else 0 end) as d5_cnt
		, sum(case when sess_visit_date = user_create_date + interval '6 day' then 1 else 0 end) as d6_cnt
		, sum(case when sess_visit_date = user_create_date + interval '7 day' then 1 else 0 end) as d7_cnt
from temp_01 group by user_create_date order by 1
)
select user_create_date, daily_create_visit_cnt,
       case when user_create_date + interval '1 day' > to_date('2016-08-05', 'yyyy-mm-dd') then null else d1_cnt end as d1_cnt,
	   case when user_create_date + interval '2 day' > to_date('2016-08-05', 'yyyy-mm-dd') then null else d2_cnt end as d2_cnt,
	   case when user_create_date + interval '3 day' > to_date('2016-08-05', 'yyyy-mm-dd') then null else d3_cnt end as d3_cnt,
	   case when user_create_date + interval '4 day' > to_date('2016-08-05', 'yyyy-mm-dd') then null else d4_cnt end as d4_cnt,
	   case when user_create_date + interval '5 day' > to_date('2016-08-05', 'yyyy-mm-dd') then null else d5_cnt end as d5_cnt
from temp_02;


-- page �� view �Ǽ� ��ȸ. ���� �Ǽ��� ���� ������ ����
select page_path, count(*) from ga_sess_hits
group by page_path order by 2 desc;

-- sessiong level���� landing page�� ���� ���, ga_sess_hits���� landing page �����ϱ�.  
with temp_01 as ( 
	select sess_id, max(first_page) as landing_page_name
	from 
	(
	select sess_id, hit_seq, page_path
		, first_value(page_path) over (partition by sess_id order by hit_seq) first_page
	from ga_sess_hits
	) a group by sess_id
)
select landing_page_name, count(*) 
from temp_01 group by landing_page_name order by 2 desc;


-- bounced session ����. 
select sess_id, count(*) from ga_sess_hits
group by sess_id having count(*) = 1;

-- sessiong level���� landing page�� ���� ���, ga_sess_hits���� landing page �����ϰ�, ��ü ���ǿ��� bounced session�Ǽ��� ���� ����. 
with temp_01 as ( 
	select sess_id, max(first_page) as landing_page_name, count(*) page_cnt
	from (
		select sess_id, hit_seq, page_path
			, first_value(page_path) over (partition by sess_id order by hit_seq) first_page
		from ga_sess_hits
	) a group by sess_id
)
select sum(case when page_cnt = 1 then 1 else 0 end) as bounce_cnt, count(*) total_sess_cnt
	, 1.0*sum(case when page_cnt = 1 then 1 else 0 end)/count(*) as bounce_ratio 
from temp_01;

-- landing page �� bounce ratio
with temp_01 as ( 
	select sess_id, max(first_page) as landing_page_name, count(*) page_cnt
	from (
		select sess_id, hit_seq, page_path
			, first_value(page_path) over (partition by sess_id order by hit_seq) first_page
		from ga_sess_hits
	) a group by sess_id
)
select landing_page_name
	, sum(case when page_cnt = 1 then 1 else 0 end) as bounce_cnt
	, count(*) total_sess_cnt
	, 1.0*sum(case when page_cnt = 1 then 1 else 0 end)/count(*) as bounce_ratio
from temp_01
group by landing_page_name order by 3 desc;


-- landing page + exit page �� ���� �Ǽ�
with temp_01 as ( 
	select sess_id, max(first_page) as landing_page_name
		, max(last_page) as exit_page_name, count(*) page_cnt
	from (
		select sess_id, hit_seq, page_path
			, first_value(page_path) over (partition by sess_id order by hit_seq) first_page
			, last_value(page_path) over (partition by sess_id order by hit_seq 
			                              rows between unbounded preceding and unbounded following) as last_page
		from ga_sess_hits
	) a group by sess_id
)
select landing_page_name, exit_page_name, count(*) 
from temp_01
group by 1, 2 order by 3 desc;


-- ���ں� landing page �� ���� �Ǽ�

with temp_01 as ( 
	select sess_id, max(first_page) as landing_page_name, count(*) page_cnt
	from (
		select sess_id, hit_seq, page_path
			, first_value(page_path) over (partition by sess_id order by hit_seq) first_page
		from ga_sess_hits
	) a group by sess_id
);

-- ���ں� ���� ���Ӽ��� ���� ���� top3 landing page�� ���� �Ǽ� ����
with temp_01 as (
	select visit_date, first_page as landing_page, count(distinct sess_id) as sess_cnt 
	from (
		select a.sess_id, date_trunc('day', b.visit_stime) as visit_date 
			, first_value(page_path) over (partition by a.sess_id order by hit_seq) first_page
		from ga_sess_hits a
			join ga_sess b 
				on a.sess_id = b.sess_id
	) a group by 1, 2
)
select visit_date, landing_page, sess_cnt 
from (
	select * 
		, row_number() over (partition by visit_date order by sess_cnt desc) rnum
	from temp_01
) a where rnum <= 3;

-- ���ں� ���� ���Ӽ��� ���� ���� top3 landing page�� ���� �Ǽ� ������ pivot ���·� ����. 
with temp_01 as (
	select visit_date, first_page as landing_page, count(distinct sess_id) as sess_cnt 
	from (
		select a.sess_id, date_trunc('day', b.visit_stime) as visit_date 
			, first_value(page_path) over (partition by a.sess_id order by hit_seq) first_page
		from ga_sess_hits a
			join ga_sess b 
				on a.sess_id = b.sess_id
	) a group by 1, 2
)
select visit_date, 
	max(case when rnum = 1 then landing_page end) as landing_page_1st
	-- sum()�� max()�� �ٲپ ����
	, sum(case when rnum = 1 then sess_cnt end) as landing_page_1st_cnt
	, max(case when rnum = 2 then landing_page end) as landing_page_2nd
	, sum(case when rnum = 2 then sess_cnt end) as landing_page_2nd_cnt 
	, max(case when rnum = 3 then landing_page end) as landing_page_3rd
	, sum(case when rnum = 3 then sess_cnt end) as landing_page_3rd_cnt 
from (
	select * 
		, row_number() over (partition by visit_date order by sess_cnt desc) rnum
	from temp_01
) a where rnum <= 3
group by visit_date;



-- conversion funnel �۾� ��. 
/* 
   Unknown = 0.
   Click through of product lists = 1, 
   Product detail views = 2, 
   Add product(s) to cart = 3, 
   Remove product(s) from cart = 4, 
   Check out = 5, 
   Completed purchase = 6, 
   Refund of purchase = 7, 
   Checkout options = 8
 */
select action_type, count(*) as cnt from ga_sess_hits group by action_type order by 2 desc;

select action_type, cnt
	, sum(cnt) over ()
	, first_value(cnt) over (order by action_type)
	, lag(cnt) over ( order by action_type)
from (
	select action_type, count(*) as cnt from ga_sess_hits where action_type in ('0', '1', '2', '3', '6') group by action_type
) a;


with
temp_01 as ( 
select action_type, count(*) as cnt 
from ga_sess_hits 
group by action_type
)
select action_type, cnt, 
	lag(cnt) over (order by action_type) as prev_cnt,
	1.0 * cnt/lag(cnt) over (order by action_type) as prev_ratio
from temp_01 where action_type in ('0', '1', '2', '3', '5', '6');

-- action_type���� funnel ����. 
/* action_type�� 0 �ΰ��� �ϳ��� ���� ���ǿ��� �ſ� ���� ����. action_type=0 �� ���� ���Ǻ��� �Ѱ��� ���� ���� �ʿ� */
with
temp_01 as ( 
select action_type, count(*) as cnt 
from ga_sess_hits 
group by action_type
)
select action_type, cnt
	, lag(cnt) over (order by action_type) as prev_cnt
	, 1.0 * cnt/lag(cnt) over (order by action_type) as prev_ratio
	, first_value(cnt) over() as action_0_cnt
from temp_01 where action_type in ('0', '1', '2', '3', '5', '6')


-- pivot ���·� funnel ����. 
with
temp_01 as ( 
select action_type, count(*) as cnt 
from ga_sess_hits 
group by action_type
),
temp_02 as (
select action_type, cnt
	, lag(cnt) over (order by action_type) as prev_cnt
	, 1.0 * cnt/lag(cnt) over (order by action_type) as prev_ratio
	, first_value(cnt) over() as action_0_cnt
from temp_01 where action_type in ('0', '1', '2', '3', '5', '6')
)
select max(action_0_cnt) as action_0_cnt
	, max(case when action_type='1' then prev_ratio end) as landing_to_product_ratio
	, max(case when action_type='2' then prev_ratio end) as product_to_detail_ratio
	, max(case when action_type='3' then prev_ratio end) as detail_to_cart_ratio
	, max(case when action_type='5' then prev_ratio end) as cart_to_checkout_ratio
	, max(case when action_type='6' then prev_ratio end) as checkout_to_complete_ratio
from temp_02;

-- action_type�� �Ǽ�, action_type�� ���� ���� �Ǽ�, ���Ǻ� action type ���� �Ǽ�. 
select action_type, count(*) as action_cnt
	, count(distinct sess_id) as sess_cnt_per_action
	, 1.0*count(*)/count(distinct sess_id) as action_cnt_per_sess
from ga_sess_hits 
group by action_type;


select * from ga_sess;