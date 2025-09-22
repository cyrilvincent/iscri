with d as (
	select year, month from iscri
	group by year, month
)
select coalesce(iscri.id,0) as id, d.year as year, d.month as month, actor1.iso3 as actor1_code, actor2.iso3 as actor2_code, coalesce(iscri.iscri3,0) as iscri3, coalesce(iscri.iscri4,0) as iscri4, coalesce(iscri.iscri,0) as iscri
from country as actor1
join country as actor2 on true
join d on true --d.year=2025 --and d.month=6
left outer join iscri on iscri.actor1_code=actor1.iso3
	and iscri.actor2_code=actor2.iso3
	and iscri.year=d.year and iscri.month=d.month


