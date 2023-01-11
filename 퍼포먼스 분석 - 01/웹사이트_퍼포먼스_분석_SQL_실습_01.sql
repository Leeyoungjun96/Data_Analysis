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
