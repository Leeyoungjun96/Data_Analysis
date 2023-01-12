/************************************
Hit���� ���� ���� ���� 5�� ������(�̺�Ʈ ����)�� ���Ǵ� �ִ�, ���, 4���� ������/�̺�Ʈ Hit��
*************************************/

-- hit���� ���� ���� ���� 5�� ������(�̺�Ʈ ����)
select page_path, count(*) as hits_by_page 
from ga_sess_hits
group by page_path order by 2 desc
FETCH FIRST 5 ROW only;

-- ���Ǵ� �ִ�, ���, 4���� ������(�̺�Ʈ ����) Hit ��
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
���� 30�ϰ� �Ϻ� page hit �Ǽ� �� 30�� ��� �Ϻ� page hit
*************************************/

select date_trunc('day', b.visit_stime)::date as d_day, count(*) as page_cnt
	  -- group by�� ����� ��� ���տ� analytic avg()�� �����. 
	, round(avg(count(*)) over (), 2) as avg_page_cnt
from ga.ga_sess_hits a
	join ga.ga_sess b on a.sess_id = b.sess_id
where b.visit_stime >= (:current_date - interval '30 days') and b.visit_stime < :current_date
and a.hit_type = 'PAGE'
group by date_trunc('day', b.visit_stime)::date;

/************************************
 ���� �Ѵް� �������� ��ȸ���� �� ������(���� ���� ������) ��ȸ��
*************************************/
-- �������� ��ȸ���� �������� ��ȸ��
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
 * �Ʒ��� ���� temp_02 �� �����ص� ��. �� ��뷮 �������� ��� �ð��� �� �� �ɸ� �� ����. 
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

-- �Ʒ��� ���� �Ѵް� �������� ��ȸ���� �� ������(���� ���� ������) ��ȸ��
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
���� 30�ϰ� �������� ��� ������ �ӹ� �ð�.
���Ǻ� ������ ������(Ż�� ������)�� ��� �ð� ��꿡�� ����.  
���� ���� �� hit_seq=1�̸�(�� �Ա� ������) ������ hit_time�� 0 ��. 
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


-- �������� ��ȸ �Ǽ��� ���� ��ȸ(���Ǻ� unique ������), ��� �ӹ� �ð�(��)�� �Ѳ����� ���ϱ�
-- �������� ������ ���� ���� �� �̸� ����
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


-- �Ʒ��� ���� ���� �߰��������� ���� �����ϰ� ������ �� �ֽ��ϴ�. 
with
temp_01 as (
	select a.sess_id, a.page_path, hit_seq, hit_time
		, lead(hit_time) over (partition by a.sess_id order by hit_seq) as next_hit_time
		-- ���ǳ����� ������ page_path�� ���� ��� rnum�� 2�̻��� ��. ���Ŀ� 1���� count�� ����. 
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
ga_sess_hits ���̺��� ���� session ���� ���� ������(landing page)�� ���� ������(exit page), �׸��� �ش� page�� ���� ������ ���� �÷��� ����.
���� ������ ���δ� �ݵ�� hit_type�� PAGE�� ���� True��. 
*************************************/

with temp_01 
as(
select sess_id, hit_seq, hit_type, page_path
	--, landing_screen_name
	-- ���� sess_id ������ hit_seq�� ���� ó���� ��ġ�� page_path�� landing page
	, first_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and current row) as landing_page
	-- ���� sess_id ������ hit_seq�� ���� �������� ��ġ�� page_path�� exit page. 
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

-- �ҽ� ���ڿ��� ���ǿ� ���� ����. 
select regexp_replace(
		'shop.googlemerchandisestore.com/google+redesign/shop+by+brand/google',
		'shop.|www.',
		'');

/************************************
landing page, exit page, landing page + exit page �� page�� ���� session �Ǽ�
*************************************/
-- landing page/exit�� page�� ���� session �Ǽ�
with temp_01 
as(
select sess_id, hit_seq, action_type, hit_type, page_path
	, landing_screen_name
	, first_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and current row) as landing_page
	-- hit_type�� PAGE�� ���� last_value()�� �����ϰ�, EVENT�϶��� NULL�� ġȯ. 
	, case when hit_type='PAGE' then last_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and unbounded following)
		   else null end as exit_page
	, exit_screen_name
	, is_exit
	-- hit_type�� PAGE�̰� �� ������ hit_seq�϶��� exit page��. 
	, case when row_number() over (partition by sess_id, hit_type order by hit_seq desc) = 1 and hit_type='PAGE' then 'True' else '' end as is_exit_new
from ga_sess_hits
), 
temp_02 as (
select sess_id, hit_seq, action_type, hit_type, page_path
    , landing_screen_name, exit_screen_name
	, landing_page
	-- max() analtyic���� null ���� window ��ܰ� ���� ������. 
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

--  landing page + exit page �� page�� ���� session �Ǽ�
with temp_01 
as(
select sess_id, hit_seq, action_type, hit_type, page_path
	, landing_screen_name
	, first_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and current row) as landing_page
	-- hit_type�� PAGE�� ���� last_value()�� �����ϰ�, EVENT�϶��� NULL�� ġȯ. 
	, case when hit_type='PAGE' then last_value(page_path) over (partition by sess_id order by hit_seq 
									rows between unbounded preceding and unbounded following)
		   else null end as exit_page
	, exit_screen_name
	, is_exit
	-- hit_type�� PAGE�̰� �� ������ hit_seq�϶��� exit page��. 
	, case when row_number() over (partition by sess_id, hit_type order by hit_seq desc) = 1 and hit_type='PAGE' then 'True' else '' end as is_exit_new
from ga_sess_hits
), 
temp_02 as (
select sess_id, hit_seq, action_type, hit_type, page_path
    , landing_screen_name, exit_screen_name
	, landing_page
	-- max() analtyic���� null ���� window ��ܰ� ���� ������. 
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
��Ż��(Bounce Ratio) ����
���� ���� �� �ٸ� �������� �̵����� �ʰ� �ٷ� ������ ���� ����
��ü �������� �������� ��Ż���� ���� ��� bounce session �Ǽ�/���� session �Ǽ�
*************************************/

-- bounce session ����. 
select sess_id, count(*) from ga_sess_hits
group by sess_id having count(*) = 1;

-- bounce session ��κ��� PAGE������ �Ϻδ� EVENT�� ����. 
select sess_id, count(*), max(hit_type), min(hit_type) from ga_sess_hits
group by sess_id having count(*) = 1 and (max(hit_type) = 'EVENT' or min(hit_type) = 'EVENT');

-- ��ü ���������� ��Ż��(bounce ratio) ���ϱ�
with 
temp_01 as ( 
select sess_id, count(*) as page_cnt
from ga_sess_hits
group by sess_id
)
select sum(case when page_cnt = 1 then 1 else 0 end) as bounce_sess_cnt -- bounce session �Ǽ�
	, count(*) as sess_cnt -- ���� session �Ǽ� 
	, round(100.0*sum(case when page_cnt = 1 then 1 else 0 end)/count(*), 2) as bounce_sess_pct -- ��Ż��
from temp_01;

-- ���Ǵ� �ִ�, ���, 4���� ������(�̺�Ʈ ����) Hit ��
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

