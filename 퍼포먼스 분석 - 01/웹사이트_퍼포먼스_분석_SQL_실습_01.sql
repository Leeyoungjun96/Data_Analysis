/************************************
Hit수가 가장 많은 상위 5개 페이지(이벤트 포함)와 세션당 최대, 평균, 4분위 페이지/이벤트 Hit수
*************************************/

-- hit수가 가장 많은 상위 5개 페이지(이벤트 포함)
select page_path, count(*) as hits_by_page 
from ga_sess_hits
group by page_path order by 2 desc
FETCH FIRST 5 ROW only;

-- 세션당 최대, 평균, 4분위 페이지(이벤트 포함) Hit 수
with 
temp_01 as (
select sess_id, count(*) as hits_by_sess
from ga_sess_hits
group by sess_id 
)
select max(hits_by_sess), avg(hits_by_sess), min(hits_by_sess), count(*) as cnt
	, percentile_disc(0.25) within group(order by hits_by_sess) as percentile_25
	, percentile_disc(0.50) within group(order by hits_by_sess) as percentile_50
	, percentile_disc(0.75) within group(order by hits_by_sess) as percentile_75
	, percentile_disc(0.80) within group(order by hits_by_sess) as percentile_80
	, percentile_disc(1.0) within group(order by hits_by_sess) as percentile_100
from temp_01;

/************************************
과거 30일간 일별 page hit 건수 및 30일 평균 일별 page hit
*************************************/

select date_trunc('day', b.visit_stime)::date as d_day, count(*) as page_cnt
	  -- group by가 적용된 결과 집합에 analytic avg()가 적용됨. 
	, round(avg(count(*)) over (), 2) as avg_page_cnt
from ga.ga_sess_hits a
	join ga.ga_sess b on a.sess_id = b.sess_id
where b.visit_stime >= (:current_date - interval '30 days') and b.visit_stime < :current_date
and a.hit_type = 'PAGE'
group by date_trunc('day', b.visit_stime)::date;

/************************************
 과거 한달간 페이지별 조회수와 순 페이지(세션 고유 페이지) 조회수
*************************************/
-- 페이지별 조회수와 순페이지 조회수
with
temp_01 as (
	select page_path, count(*) as page_cnt
	from ga.ga_sess_hits 
	where hit_type = 'PAGE'
	group by page_path
), 
temp_02 as (
	select page_path, count(*) as unique_page_cnt
	from (
		select distinct sess_id, page_path
		from ga.ga_sess_hits 
		where hit_type = 'PAGE'
	) a group by page_path
)
select a.page_path, page_cnt, unique_page_cnt
from temp_01 a
	join temp_02 b on a.page_path = b.page_path
order by 2 desc;

/*
 * 아래와 같이 temp_02 를 구성해도 됨. 단 대용량 데이터의 경우 시간이 좀 더 걸릴 수 있음. 
 * temp_02 as (
	select page_path, count(*) as unique_page_cnt
	from (
		select sess_id, page_path
			, row_number() over (partition by sess_id, page_path order by page_path) as rnum
		from ga.ga_sess_hits 
		where hit_type = 'PAGE'
	) a 
	where rnum = 1 
    group by page_path
)
 */

-- 아래는 과거 한달간 페이지별 조회수와 순 페이지(세션 고유 페이지) 조회수
with
temp_01 as (
	select a.page_path, count(*) as page_cnt
	from ga.ga_sess_hits a
		join ga.ga_sess b on a.sess_id = b.sess_id 
	where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
	where hit_type = 'PAGE'
	group by page_path
), 
temp_02 as (
	select page_path, count(*) as unique_page_cnt
	from (
		select distinct a.sess_id, a.page_path
		from ga.ga_sess_hits a
			join ga.ga_sess b on a.sess_id = b.sess_id 
		where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
		where hit_type = 'PAGE'
	) a group by page_path
)
select a.page_path, page_cnt, unique_page_cnt
from temp_01 a
	join temp_02 b on a.page_path = b.page_path
order by 2 desc;


/************************************
과거 30일간 페이지별 평균 페이지 머문 시간.
세션별 마지막 페이지(탈출 페이지)는 평균 시간 계산에서 제외.  
세션 시작 시 hit_seq=1이면(즉 입구 페이지) 무조건 hit_time이 0 임. 
*************************************/
select * 
from ga_sess_hits
where hit_seq = 1 and hit_time != 0;

with 
temp_01 as (
select sess_id, page_path, hit_seq, hit_time
	, lead(hit_time) over (partition by sess_id order by hit_seq) as next_hit_time
from ga.ga_sess_hits 
where hit_type = 'PAGE'
)
select page_path, count(*) as page_cnt
	, round(avg(next_hit_time - hit_time)/1000, 2) as avg_elapsed_sec
from temp_01
group by page_path order by 2 desc;


-- 페이지별 조회 건수와 순수 조회(세션별 unique 페이지), 평균 머문 시간(초)를 한꺼번에 구하기
-- 개별적인 집합을 각각 만든 뒤 이를 조인
with
temp_01 as (
	select a.page_path, count(*) as page_cnt
	from ga.ga_sess_hits a
		join ga.ga_sess b on a.sess_id = b.sess_id 
	where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
	and a.hit_type = 'PAGE'
	group by page_path
), 
temp_02 as (
	select page_path, count(*) as unique_page_cnt
	from (
		select distinct a.sess_id, a.page_path
		from ga.ga_sess_hits a
			join ga.ga_sess b on a.sess_id = b.sess_id 
		where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
		and a.hit_type = 'PAGE'
	) a group by page_path
), 
temp_03 as (
	select a.sess_id, page_path, hit_seq, hit_time
		, lead(hit_time) over (partition by a.sess_id order by hit_seq) as next_hit_time
	from ga.ga_sess_hits a
		join ga.ga_sess b on a.sess_id = b.sess_id 
	where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
	and a.hit_type = 'PAGE'
), 
temp_04 as (
select page_path, count(*) as page_cnt
	, round(avg(next_hit_time - hit_time)/1000.0, 2) as avg_elapsed_sec
from temp_03
group by page_path
)
select a.page_path, a.page_cnt, b.unique_page_cnt, c.avg_elapsed_sec
from temp_01 a
	left join temp_02 b on a.page_path = b.page_path
	left join temp_04 c on a.page_path = c.page_path
order by 2 desc;


-- 아래와 같이 공통 중간집합으로 보다 간단하게 추출할 수 있습니다. 
with
temp_01 as (
	select a.sess_id, a.page_path, hit_seq, hit_time
		, lead(hit_time) over (partition by a.sess_id order by hit_seq) as next_hit_time
		-- 세션내에서 동일한 page_path가 있을 경우 rnum은 2이상이 됨. 추후에 1값만 count를 적용. 
		, row_number() over (partition by a.sess_id, page_path order by hit_seq) as rnum
	from ga.ga_sess_hits a
		join ga_sess b on a.sess_id = b.sess_id 
	where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
	and a.hit_type = 'PAGE'
)
select page_path, count(*) as page_cnt
	, count(case when rnum = 1 then '1' else null end) as unique_page_cnt
	, round(avg(next_hit_time - hit_time)/1000.0, 2) as avg_elapsed_sec
from temp_01
group by page_path order by 2 desc;


/************************************
ga_sess_hits 테이블에서 개별 session 별로 진입 페이지(landing page)와 종료 페이지(exit page), 그리고 해당 page의 종료 페이지 여부 컬럼을 생성.
종료 페이지 여부는 반드시 hit_type이 PAGE일 때만 True임. 
*************************************/

with temp_01 
as(
select sess_id, hit_seq, hit_type, page_path
	--, landing_screen_name
	-- 동일 sess_id 내에서 hit_seq가 가장 처음에 위치한 page_path가 landing page
	, first_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and current row) as landing_page
	-- 동일 sess_id 내에서 hit_seq가 가장 마지막에 위치한 page_path가 exit page. 
	, last_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and unbounded following) as exit_page
	--, exit_screen_name
	--, is_exit
	, case when row_number() over (partition by sess_id, hit_type order by hit_seq desc) = 1 and hit_type='PAGE' then 'True' else '' end as is_exit_new
	--, case when row_number() over (partition by sess_id, hit_type order by hit_seq desc) = 1 then 'True' else '' end as is_exit_new
from ga_sess_hits
)
select * 
from temp_01 
--where is_exit_new != is_exit
--where is_exit = 'True' and hit_type = 'EVENT'
--where 'googlemerchandisestore.com'||exit_page != regexp_replace(exit_screen_name, 'shop.|www.', '')

-- 소스 문자열을 조건에 따라 변경. 
select regexp_replace(
		'shop.googlemerchandisestore.com/google+redesign/shop+by+brand/google',
		'shop.|www.',
		'');

/************************************
landing page, exit page, landing page + exit page 별 page와 고유 session 건수
*************************************/
-- landing page/exit별 page와 고유 session 건수
with temp_01 
as(
select sess_id, hit_seq, action_type, hit_type, page_path
	, landing_screen_name
	, first_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and current row) as landing_page
	-- hit_type이 PAGE일 때만 last_value()를 적용하고, EVENT일때는 NULL로 치환. 
	, case when hit_type='PAGE' then last_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and unbounded following)
		   else null end as exit_page
	, exit_screen_name
	, is_exit
	-- hit_type이 PAGE이고 맨 마지막 hit_seq일때만 exit page임. 
	, case when row_number() over (partition by sess_id, hit_type order by hit_seq desc) = 1 and hit_type='PAGE' then 'True' else '' end as is_exit_new
from ga_sess_hits
), 
temp_02 as (
select sess_id, hit_seq, action_type, hit_type, page_path
    , landing_screen_name, exit_screen_name
	, landing_page
	-- max() analtyic으로 null 값을 window 상단값 부터 복제함. 
	, max(exit_page) over (partition by sess_id) as exit_page
	, is_exit, is_exit_new
from temp_01
)
select 
	--landing_page, count(*) as page_cnt, count(distinct sess_id) as sess_cnt
     exit_page, count(*) as page_cnt, count(distinct sess_id) as sess_cnt
from temp_02 
--group by landing_page order by 2 desc 
 group by exit_page order by 2 desc
;

--  landing page + exit page 별 page와 고유 session 건수
with temp_01 
as(
select sess_id, hit_seq, action_type, hit_type, page_path
	, landing_screen_name
	, first_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and current row) as landing_page
	-- hit_type이 PAGE일 때만 last_value()를 적용하고, EVENT일때는 NULL로 치환. 
	, case when hit_type='PAGE' then last_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and unbounded following)
		   else null end as exit_page
	, exit_screen_name
	, is_exit
	-- hit_type이 PAGE이고 맨 마지막 hit_seq일때만 exit page임. 
	, case when row_number() over (partition by sess_id, hit_type order by hit_seq desc) = 1 and hit_type='PAGE' then 'True' else '' end as is_exit_new
from ga_sess_hits
), 
temp_02 as (
select sess_id, hit_seq, action_type, hit_type, page_path
    , landing_screen_name, exit_screen_name
	, landing_page
	-- max() analtyic으로 null 값을 window 상단값 부터 복제함. 
	, max(exit_page) over (partition by sess_id) as exit_page
	, is_exit, is_exit_new
from temp_01
)
select 
     landing_page, exit_page, count(*) as page_cnt, count(distinct sess_id) as sess_cnt
from temp_02 
group by landing_page, exit_page order by 3 desc
;

/************************************
이탈율(Bounce Ratio) 추출
최초 접속 후 다른 페이지로 이동하지 않고 바로 종료한 세션 비율
전체 페이지를 기준으로 이탈율을 구할 경우 bounce session 건수/고유 session 건수
*************************************/

-- bounce session 추출. 
select sess_id, count(*) from ga_sess_hits
group by sess_id having count(*) = 1;

-- bounce session 대부분은 PAGE이지만 일부는 EVENT도 존재. 
select sess_id, count(*), max(hit_type), min(hit_type) from ga_sess_hits
group by sess_id having count(*) = 1 and (max(hit_type) = 'EVENT' or min(hit_type) = 'EVENT');

-- 전체 페이지에서 이탈율(bounce ratio) 구하기
with 
temp_01 as ( 
select sess_id, count(*) as page_cnt
from ga_sess_hits
group by sess_id
)
select sum(case when page_cnt = 1 then 1 else 0 end) as bounce_sess_cnt -- bounce session 건수
	, count(*) as sess_cnt -- 고유 session 건수 
	, round(100.0*sum(case when page_cnt = 1 then 1 else 0 end)/count(*), 2) as bounce_sess_pct -- 이탈율
from temp_01;

-- 세션당 최대, 평균, 4분위 페이지(이벤트 포함) Hit 수
with 
temp_01 as (
select sess_id, count(*) as hits_by_sess
from ga_sess_hits
group by sess_id 
)
select max(hits_by_sess), avg(hits_by_sess), min(hits_by_sess), count(*) as cnt
	, percentile_disc(0.25) within group(order by hits_by_sess) as percentile_25
	, percentile_disc(0.50) within group(order by hits_by_sess) as percentile_50
	, percentile_disc(0.75) within group(order by hits_by_sess) as percentile_75
	, percentile_disc(0.80) within group(order by hits_by_sess) as percentile_80
	, percentile_disc(1.0) within group(order by hits_by_sess) as percentile_100
from temp_01;

