select distinct(actor1_code) from iscri
left join country on iso3 = actor1_code
where iso3 is null

select * from event where actor1_country_code = 'CAS' limit 1