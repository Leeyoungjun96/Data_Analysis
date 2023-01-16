
/************************************
사용자 생성 날짜 별 일주일간 잔존율(Retention rate) 구하기
*************************************/
with temp_01 as (
	select a.user_id, date_trunc('day', a.create_time)::date as user_create_date,  date_trunc('day', b.visit_stime)::date as sess_visit_date
		, count(*) cnt
	from ga_users a
		left join ga_sess b
			on a.user_id = b.user_id
	where  create_time >= (:current_date - interval '8 days') and create_time < :current_date
	group by a.user_id, date_trunc('day', a.create_time)::date, date_trunc('day', b.visit_stime)::date
),
temp_02 as (
select user_create_date, count(*) as create_cnt
	-- d1 에서 d7 일자별 접속 사용자 건수 구하기. 
	, sum(case when sess_visit_date = user_create_date + interval '1 day' then 1 else 0 end ) as d1_cnt
	, sum(case when sess_visit_date = user_create_date + interval '2 day' then 1 else 0 end) as d2_cnt
	, sum(case when sess_visit_date = user_create_date + interval '3 day' then 1 else 0 end) as d3_cnt
	, sum(case when sess_visit_date = user_create_date + interval '4 day' then 1 else 0 end) as d4_cnt
	, sum(case when sess_visit_date = user_create_date + interval '5 day' then 1 else 0 end) as d5_cnt
	, sum(case when sess_visit_date = user_create_date + interval '6 day' then 1 else 0 end) as d6_cnt
	, sum(case when sess_visit_date = user_create_date + interval '7 day' then 1 else 0 end) as d7_cnt
	/*
	, sum(case when sess_visit_date = user_create_date + interval '1 day' then 1 else null end ) as d1_cnt
	, sum(case when sess_visit_date = user_create_date + interval '2 day' then 1 else null end) as d2_cnt
	, sum(case when sess_visit_date = user_create_date + interval '3 day' then 1 else null end) as d3_cnt
	, sum(case when sess_visit_date = user_create_date + interval '4 day' then 1 else null end) as d4_cnt
	, sum(case when sess_visit_date = user_create_date + interval '5 day' then 1 else null end) as d5_cnt
	, sum(case when sess_visit_date = user_create_date + interval '6 day' then 1 else null end) as d6_cnt
	, sum(case when sess_visit_date = user_create_date + interval '7 day' then 1 else null end) as d7_cnt
	*/
from temp_01 
group by user_create_date
)
select user_create_date, create_cnt
     -- d1 에서 d7 일자별 잔존율 구하기.
	, round(100.0 * d1_cnt/create_cnt, 2) as d1_ratio
	, round(100.0 * d2_cnt/create_cnt, 2) as d2_ratio
	, round(100.0 * d3_cnt/create_cnt, 2) as d3_ratio
	, round(100.0 * d4_cnt/create_cnt, 2) as d4_ratio
	, round(100.0 * d5_cnt/create_cnt, 2) as d5_ratio
	, round(100.0 * d6_cnt/create_cnt, 2) as d6_ratio
	, round(100.0 * d7_cnt/create_cnt, 2) as d7_ratio
from temp_02 order by 1;

/************************************
주별 잔존율(Retention rate) 및 주별 특정 채널 잔존율
*************************************/
with temp_01 as (
	select a.user_id, date_trunc('week', a.create_time)::date as user_create_date,  date_trunc('week', b.visit_stime)::date as sess_visit_date
		, count(*) cnt
	from ga_users a
		left join ga_sess b
			on a.user_id = b.user_id
	--where  create_time >= (:current_date - interval '7 weeks') and create_time < :current_date
	where create_time >= to_date('20160912', 'yyyymmdd') and create_time < to_date('20161101', 'yyyymmdd')
	group by a.user_id, date_trunc('week', a.create_time)::date, date_trunc('week', b.visit_stime)::date
), 
temp_02 as (
select user_create_date, count(*) as create_cnt
     -- w1 에서 w7까지 주단위 접속 사용자 건수 구하기.
	, sum(case when sess_visit_date = user_create_date + interval '1 week' then 1 else null end ) as w1_cnt
	, sum(case when sess_visit_date = user_create_date + interval '2 week' then 1 else null end) as w2_cnt
	, sum(case when sess_visit_date = user_create_date + interval '3 week' then 1 else null end) as w3_cnt
	, sum(case when sess_visit_date = user_create_date + interval '4 week' then 1 else null end) as w4_cnt
	, sum(case when sess_visit_date = user_create_date + interval '5 week' then 1 else null end) as w5_cnt
	, sum(case when sess_visit_date = user_create_date + interval '6 week' then 1 else null end) as w6_cnt
	, sum(case when sess_visit_date = user_create_date + interval '7 week' then 1 else null end) as w7_cnt
from temp_01 
group by user_create_date
)
select user_create_date, create_cnt
    -- w1 에서 w7 주별 잔존율 구하기.
	, round(100.0 * w1_cnt/create_cnt, 2) as w1_ratio
	, round(100.0 * w2_cnt/create_cnt, 2) as w2_ratio
	, round(100.0 * w3_cnt/create_cnt, 2) as w3_ratio
	, round(100.0 * w4_cnt/create_cnt, 2) as w4_ratio
	, round(100.0 * w5_cnt/create_cnt, 2) as w5_ratio
	, round(100.0 * w6_cnt/create_cnt, 2) as w6_ratio
	, round(100.0 * w7_cnt/create_cnt, 2) as w7_ratio
from temp_02 order by 1;

-- 주 단위 특정 채널 잔존율(Retention rate)
with temp_01 as (
	select a.user_id, date_trunc('week', a.create_time)::date as user_create_date,  date_trunc('week', b.visit_stime)::date as sess_visit_date
		, count(*) cnt
	from ga_users a
		left join ga_sess b
			on a.user_id = b.user_id
	--where  create_time >= (:current_date - interval '7 weeks') and create_time < :current_date
	where create_time >= to_date('20160912', 'yyyymmdd') and create_time < to_date('20161101', 'yyyymmdd')
	and channel_grouping='Referral' -- Social Organic Search, Direct, Referral
	group by a.user_id, date_trunc('week', a.create_time)::date, date_trunc('week', b.visit_stime)::date
), 
temp_02 as (
select user_create_date, count(*) as create_cnt
	-- w1 에서 w7까지 주단위 접속 사용자 건수 구하기.
	, sum(case when sess_visit_date = user_create_date + interval '1 week' then 1 else null end ) as w1_cnt
	, sum(case when sess_visit_date = user_create_date + interval '2 week' then 1 else null end) as w2_cnt
	, sum(case when sess_visit_date = user_create_date + interval '3 week' then 1 else null end) as w3_cnt
	, sum(case when sess_visit_date = user_create_date + interval '4 week' then 1 else null end) as w4_cnt
	, sum(case when sess_visit_date = user_create_date + interval '5 week' then 1 else null end) as w5_cnt
	, sum(case when sess_visit_date = user_create_date + interval '6 week' then 1 else null end) as w6_cnt
	, sum(case when sess_visit_date = user_create_date + interval '7 week' then 1 else null end) as w7_cnt
from temp_01 
group by user_create_date
)
select user_create_date, create_cnt
     -- w1 에서 w7 주별 잔존율 구하기.
	, round(100.0 * w1_cnt/create_cnt, 2) as w1_ratio
	, round(100.0 * w2_cnt/create_cnt, 2) as w2_ratio
	, round(100.0 * w3_cnt/create_cnt, 2) as w3_ratio
	, round(100.0 * w4_cnt/create_cnt, 2) as w4_ratio
	, round(100.0 * w5_cnt/create_cnt, 2) as w5_ratio
	, round(100.0 * w6_cnt/create_cnt, 2) as w6_ratio
	, round(100.0 * w7_cnt/create_cnt, 2) as w7_ratio
from temp_02 order by 1;

/************************************
 (2016년 9월 12일 부터) 일주일간 생성된 사용자들에 대해 채널별 주 단위 잔존율(Retention rate)
*************************************/
with temp_01 as (
	select a.user_id, channel_grouping
		, date_trunc('week', a.create_time)::date as user_create_date,  date_trunc('week', b.visit_stime)::date as sess_visit_date
		, count(*) cnt
	from ga_users a
		left join ga_sess b
			on a.user_id = b.user_id
	where  create_time >= to_date('20160912', 'yyyymmdd') and create_time < to_date('20160919', 'yyyymmdd')
	--and channel_grouping='Referral' -- Social Organic Search, Direct, Referral
	group by a.user_id, channel_grouping, date_trunc('week', a.create_time)::date, date_trunc('week', b.visit_stime)::date
), 
temp_02 as (
select user_create_date, channel_grouping, count(*) as create_cnt
     -- w1 에서 w7까지 주단위 접속 사용자 건수 구하기.
	, sum(case when sess_visit_date = user_create_date + interval '1 week' then 1 else null end ) as w1_cnt
	, sum(case when sess_visit_date = user_create_date + interval '2 week' then 1 else null end) as w2_cnt
	, sum(case when sess_visit_date = user_create_date + interval '3 week' then 1 else null end) as w3_cnt
	, sum(case when sess_visit_date = user_create_date + interval '4 week' then 1 else null end) as w4_cnt
	, sum(case when sess_visit_date = user_create_date + interval '5 week' then 1 else null end) as w5_cnt
	, sum(case when sess_visit_date = user_create_date + interval '6 week' then 1 else null end) as w6_cnt
	, sum(case when sess_visit_date = user_create_date + interval '7 week' then 1 else null end) as w7_cnt
from temp_01 
group by user_create_date, channel_grouping
)
select user_create_date, channel_grouping, create_cnt
    -- w1 에서 w7 주별 잔존율 구하기
	, round(100.0 * w1_cnt/create_cnt, 2) as w1_ratio
	, round(100.0 * w2_cnt/create_cnt, 2) as w2_ratio
	, round(100.0 * w3_cnt/create_cnt, 2) as w3_ratio
	, round(100.0 * w4_cnt/create_cnt, 2) as w4_ratio
	, round(100.0 * w5_cnt/create_cnt, 2) as w5_ratio
	, round(100.0 * w6_cnt/create_cnt, 2) as w6_ratio
	, round(100.0 * w7_cnt/create_cnt, 2) as w7_ratio
from temp_02 order by 3 desc;

/************************************
 7일간 생성된 총 사용자를 기반으로 총 잔존율을 구하고, 7일간 일별 잔존율을 함께 구하기 
*************************************/
-- 7일간 생성된 총 사용자를 기반으로 총 잔존율을 구하고, 7일간 일별 잔존율을 함께 구하기 
with temp_01 as (
	select a.user_id, date_trunc('day', a.create_time) as user_create_date,  date_trunc('day', b.visit_stime) as sess_visit_date
		, count(*) cnt
	from ga_users a
		left join ga_sess b
			on a.user_id = b.user_id
	where  create_time >= (:current_date - interval '8 days') and create_time < :current_date
	group by a.user_id, date_trunc('day', a.create_time), date_trunc('day', b.visit_stime)
),
temp_02 as (
select user_create_date, count(*) as create_cnt
	, sum(case when sess_visit_date = user_create_date + interval '1 day' then 1 else null end ) as d1_cnt
	, sum(case when sess_visit_date = user_create_date + interval '2 day' then 1 else null end) as d2_cnt
	, sum(case when sess_visit_date = user_create_date + interval '3 day' then 1 else null end) as d3_cnt
	, sum(case when sess_visit_date = user_create_date + interval '4 day' then 1 else null end) as d4_cnt
	, sum(case when sess_visit_date = user_create_date + interval '5 day' then 1 else null end) as d5_cnt
	, sum(case when sess_visit_date = user_create_date + interval '6 day' then 1 else null end) as d6_cnt
	, sum(case when sess_visit_date = user_create_date + interval '7 day' then 1 else null end) as d7_cnt
from temp_01
group by user_create_date
)
-- 7일간 생성된 총 사용자를 기반으로 총 잔존율을 구하기
select 'All User' as user_create_date, sum(create_cnt) as create_cnt
	, round(100.0 * sum(d1_cnt)/sum(create_cnt), 2) as d1_ratio
	, round(100.0 * sum(d2_cnt)/sum(create_cnt), 2) as d2_ratio
	, round(100.0 * sum(d3_cnt)/sum(create_cnt), 2) as d3_ratio
	, round(100.0 * sum(d4_cnt)/sum(create_cnt), 2) as d4_ratio
	, round(100.0 * sum(d5_cnt)/sum(create_cnt), 2) as d5_ratio
	, round(100.0 * sum(d6_cnt)/sum(create_cnt), 2) as d6_ratio
	, round(100.0 * sum(d7_cnt)/sum(create_cnt), 2) as d7_ratio
from temp_02
union all
-- 7일간 일별 잔존율
select to_char(user_create_date, 'yyyy-mm-dd') as user_create_date, create_cnt
	, round(100.0 * d1_cnt/create_cnt, 2) as d1_ratio
	, round(100.0 * d2_cnt/create_cnt, 2) as d2_ratio
	, round(100.0 * d3_cnt/create_cnt, 2) as d3_ratio
	, round(100.0 * d4_cnt/create_cnt, 2) as d4_ratio
	, round(100.0 * d5_cnt/create_cnt, 2) as d5_ratio
	, round(100.0 * d6_cnt/create_cnt, 2) as d6_ratio
	, round(100.0 * d7_cnt/create_cnt, 2) as d7_ratio
from temp_02 order by 1;


/**********************************************
 전체 매출 전환율 및 일별, 월별 매출 전환율과 매출액
***********************************************/
/* 
   Unknown = 0. (홈페이지)
   Click through of product lists = 1, (상품 목록 선택)
   Product detail views = 2, (상품 상세 선택)
   Add product(s) to cart = 3, (카트에 상품 추가)
   Remove product(s) from cart = 4, (카트에서 상품 제거)
   Check out = 5, (결재 시작)
   Completed purchase = 6, (구매 완료)
   Refund of purchase = 7, (환불)
   Checkout options = 8 (결재 옵션 선택)
   
   이 중 1, 3, 4가 주로 EVENT로 발생. 0, 2, 5, 6은 주로 PAGE로 발생. 
 *
 **/

-- action_type별 hit_type에 따른 건수
select action_type, count(*) action_cnt
	, sum(case when hit_type='PAGE' then 1 else 0 end) as page_action_cnt
	, sum(case when hit_type='EVENT' then 1 else 0 end) as event_action_cnt
from ga.ga_sess_hits
group by action_type
;