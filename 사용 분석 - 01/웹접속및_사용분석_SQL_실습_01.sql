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
DAU, WAU, MAU 구하기
*************************************/
/* 아래는 이미 많은 과거 데이터가 있을 경우를 가정하고 DAU, WAU, MAU를 추출함 */

-- 일별 방문한 고객 수(DAU)
select date_trunc('day', visit_stime)::date as d_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
--where visit_stime between to_date('2016-10-25', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('day', visit_stime)::date;

-- 주별 방문한 고객수(WAU)
select date_trunc('week', visit_stime)::date as week_d기y, count(distinct user_id) as user_cnt
from ga.ga_sess
--where visit_stime between to_date('2016-10-24', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('week', visit_stime)::date order by 1;

-- 월별 방문한 고객수(MAU)
select date_trunc('month', visit_stime)::date as month_day, count(distinct user_id) as user_cnt 
from ga.ga_sess 
--where visit_stime between to_date('2016-10-2', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('month', visit_stime)::date;

/* 아래는 하루 주기로 계속 DAU, WAU(이전 7일), MAU(이전 30일)를 계속 추출. */

-- interval로 전일 7일 구하기
select to_date('20161101', 'yyyymmdd') - interval '7 days';

-- 현재 일을 기준으로 전 7일의 WAU 구하기
select :current_date, count(distinct user_id) as wau
from ga_sess
where visit_stime >= (:current_date - interval '7 days') and visit_stime < :current_date;

-- 현재 일을 기준으로 전일의 DAU 구하기
select :current_date, count(distinct user_id) as dau
from ga_sess
where visit_stime >= (:current_date - interval '1 days') and visit_stime < :current_date;

-- 날짜별로 DAU, WAU, MAU 값을 가지는 테이블 생성. 
create table if not exists daily_acquisitions
(d_day date,
dau integer,
wau integer,
mau integer
);

--daily_acquisitions 테이블에 지정된 current_date별 DAU, WAU, MAU을 입력
insert into daily_acquisitions
select 
	:current_date, 
	-- scalar subquery는 select 절에 사용가능하면 단 한건, 한 컬럼만 추출되어야 함. 
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
-- 데이터 입력 확인. 
select * from daily_acquisitions;


-- 과거 일자별로 DAU 생성
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as dau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '1 days') and visit_stime < b.current_date
group by b.current_date;

-- 과거 일자별로 지난 7일 WAU 생성. 
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as wau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '7 days') and visit_stime < b.current_date
group by b.current_date;

-- 과거 일자별로 지난 30일의 MAU 설정. 
with 
temp_00 as (
select generate_series('2016-08-02'::date , '2016-11-01'::date, '1 day'::interval)::date as current_date
)
select b.current_date, count(distinct user_id) as mau
from ga_sess a
	cross join temp_00 b
where visit_stime >= (b.current_date - interval '30 days') and visit_stime < b.current_date
group by b.current_date;


--데이터 확인 81587, 80693, 80082
select count(distinct user_id) as mau
	from ga_sess
	where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date;


-- 과거 일자별로 DAU 생성하는 임시 테이블 생성
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

-- 과거 일자별로 WAU 생성하는 임시 테이블 생성
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

-- 과거 일자별로 MAU 생성하는 임시 테이블 생성
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

-- DAU, WAU, MAU 임시테이블을 일자별로 조인하여 daily_acquisitions 테이블 생성. 
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

-- 아래와 같이 current_date 컬럼명을 curr_date로 수정합니다. 
create table daily_acquisitions
as
select a.current_date as curr_date, a.dau, b.wau, c.mau
from daily_dau a
	join daily_wau b on a.current_date = b.current_date
	join daily_mau c on a.current_date = c.current_date
;

/************************************
DAU와 MAU의 비율. 고착도(stickiness)  월간 사용자들중 얼마나 많은 사용자가 주기적으로 방문하는가? 재방문 지표로 서비스의 활성화 지표 제공.
*************************************/
--DAU와 MAU의 비율 
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

-- 일주일간 stickiess, 평균 stickness
select *, round(100.0 * dau/mau, 2) as stickieness
	, round(avg(100.0 * dau/mau) over(), 2) as avg_stickieness
from ga.daily_acquisitions
where curr_date between to_date('2016-10-25', 'yyyy-mm-dd') and to_date('2016-10-31', 'yyyy-mm-dd')



/************************************
사용자별 월별 세션 접속 횟수 구간별 분포 집계
step 1: 사용자별 월별 접속 횟수, (월말 3일 이전 생성된 사용자 제외) 
step 2: 사용자별 월별 접속 횟수 구간별 분포 . 월별 + 접속 횟수 구간별로 Group by
step 3: gubun 별로 pivot 하여 추출
*************************************/
 
-- user 생성일자가 해당 월의 마지막 일에서 3일전인 user 추출. 
-- 월의 마지막 일자 구하기. 
-- postgresql은 last_day()함수가 없음. 때문에 해당 일자가 속한 달의 첫번째 날짜 가령 10월 5일이면 10월 1일에 1달을 더하고 거기에 1일을 뺌
-- 즉 10월 5일 -> 10월 1일 -> 11월 1일 -> 10월 31일 순으로 계산함.

select user_id, create_time, (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date
from ga.ga_users
where create_time <= (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date - 2;

-- 사용자별 월별 세션접속 횟수, 월말 3일 이전 생성된 사용자 제외 
select a.user_id, date_trunc('month', visit_stime)::date as month
	-- 사용자별 접속 건수. 고유 접속 건수가 아니므로 count(distinct user_id)를 적용하지 않음. 
	, count(*) as monthly_user_cnt  
from ga_sess a 
	join ga_users b on a.user_id = b.user_id 
where b.create_time <= (date_trunc('month', b.create_time) + interval '1 month' - interval '1 day')::date - 2
group by a.user_id, date_trunc('month', visit_stime)::date;

-- 사용자별 월별 세션 접속 횟수 구간별 집계, 월말 3일 이전 생성된 사용자 제외 
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

-- gubun 별로 pivot 하여 추출 
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
