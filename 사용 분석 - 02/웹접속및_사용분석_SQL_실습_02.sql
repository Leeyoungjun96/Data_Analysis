/************************************
일별 세션건수, 일별 방문 사용자(유저), 사용자별 평균 세션 수
*************************************/
with temp_01 as 
(
	select to_char(date_trunc('day', visit_stime), 'yyyy-mm-dd') as d_day
		-- ga_sess 테이블에는 sess_id로 unique하므로 count(sess_id)와 동일
		, count(distinct sess_id) as daily_sess_cnt
		, count(sess_id) as daily_sess_cnt_again
		, count(distinct user_id) as daily_user_cnt 
	from ga.ga_sess group by to_char(date_trunc('day', visit_stime), 'yyyy-mm-dd')
)
select * 
	, 1.0*daily_sess_cnt/daily_user_cnt as avg_user_sessions
	-- 아래와 같이 정수와 정수를 나눌 때 postgresql은 정수로 형변환 함. 1.0을 곱해주거나 명시적으로 float type선언 
	--, daily_sess_cnt/daily_user_cnt
from temp_01;

/************************************
DAU, WAU, MAU 및 전주 대비 WAU 비율
*************************************/
-- 일별 방문한 고객 수(DAU)
select date_trunc('day', visit_stime)::date as d_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
group by date_trunc('day', visit_stime)::date;

-- 주별 방문한 고객수(WAU)
select date_trunc('week', visit_stime)::date as week_day, count(distinct user_id) as user_cnt
from ga.ga_sess 
group by date_trunc('week', visit_stime)::date order by 1;

-- 월별 방문한 고객수(MAU)
select date_trunc('month', visit_stime)::date as month_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
group by date_trunc('month', visit_stime)::date;

-- 전주 대비 WAU 비율
with temp_01 as (
	select date_trunc('week', visit_stime)::date as week_day
		, count(distinct user_id) as user_cnt
	from ga.ga_sess group by date_trunc('week', visit_stime)::date
)
select week_day, user_cnt
	-- 만일 이전 데이터가 없으면 현재 user_cnt를 가져옴. 
	, coalesce(lag(user_cnt) over (order by week_day), user_cnt) as prev_user_cnt
	-- 만일 이전 데이터가 없으면 100
	, coalesce(round(100.0 * user_cnt/lag(user_cnt) over (order by week_day), 2), 100.0) as prev_pct
from temp_01;

/************************************
DAU와 MAU의 비율. stickiness 월간 사용자들중 얼마나 어제 재 접속했는가? 재방문 지표로 서비스의 활성화 지표 제공.  
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

-- 전체 기간중 고객 별로 가장 많이 방문한 순으로 조회. 
select user_id, count(*)
from ga_sess group by user_id order by 2 desc;

-- 특정 횟수 구간별로 방문한 고객 수 - 8월 한달간 1회, 2-3회, 4-8, 9-14, 15-25, 26회 이상 방문한 고객 건수 조회.
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

-- 사용자는 최소 8월말 기준으로 3일 전에 생성되어야 함. 8월 한달간 1회, 2-3회, 4-8, 9-14, 15-25, 26회 이상 방문한 고객 건수 조회
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


/* 월별 특정 횟수 구간별로 방문한 고객 수 구하기 
  아래 스텝별로 생성. 
1. 사용자별 월별 접속 횟수, 월말 3일 이전 생성된 사용자 제외 
2.  사용자별 월별 접속 구간별 횟수, 월말 3일 이전 생성된 사용자 제외
3.  gubun 별로 pivot 하여 추출
*/

-- user 생성일자가 해당 월의 마지막 일에서 3일전인 user 추출. 
select user_id, create_time, (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date
from ga_users
where create_time <= (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date - 2;

-- 사용자별 월별 접속 횟수, 월말 3일 이전 생성된 사용자 제외 
select a.user_id, date_trunc('month', visit_stime)::date as month, count(*) as monthly_user_cnt  
from ga_sess a 
	join ga_users b 
	on a.user_id = b.user_id 
where b.create_time <= (date_trunc('month', b.create_time) + interval '1 month' - interval '1 day')::date - 2
group by a.user_id, date_trunc('month', visit_stime)::date;

