select date, count(*) from event
where month_year = 201502
and is_root_event is true
--and quad_class >= 3
and (goldstein_scale is null or goldstein_scale < 0)
-- and avg_tone < 0
and (actor1_type1_code = 'GOV'
or actor2_type1_code = 'GOV')
and actor1_country_code = 'USA'
and actor2_country_code = 'CHN'
group by date


select date, avg(num_articles) from event
where month_year = 201502
and is_root_event is true
and quad_class >= 3
and goldstein_scale < 0
and avg_tone < 0
and actor1_type1_code = 'GOV'
and actor2_type1_code = 'GOV'
and actor1_country_code = 'USA'
and actor2_country_code = 'CHN'
group by date

select total.date, count(total.*) neg, total.c tot, count(total.*)::float/total.c ratio  from event
full outer join (
	select date, count(*) c from event
	where  month_year = 201507
	and is_root_event is true
	and (actor1_type1_code = 'GOV'
	or actor2_type1_code = 'GOV')
	and actor1_country_code = 'USA'
	and actor2_country_code = 'CHN'
	group by date
) total on total.date = event.date
where  month_year = 201507
and is_root_event is true
and quad_class >= 3
and (goldstein_scale is null or goldstein_scale < 0)
and avg_tone < 0
and (actor1_type1_code = 'GOV'
or actor2_type1_code = 'GOV')
and actor1_country_code = 'USA'
and actor2_country_code = 'CHN'
group by total.date, total.c

select total.date, count(event.*) events_nb_neg, total.events_nb events_nb, sum(event.num_articles) articles_nb_neg, total.articles_nb articles_nb, avg(event.num_articles) articles_avg_neg, total.articles_avg articles_avg from event
full outer join (
	select date, count(*) events_nb, sum(num_articles) articles_nb, avg(num_articles) articles_avg from event
	where  month_year = 201507
	and is_root_event is true
	and (actor1_type1_code = 'GOV'
	or actor2_type1_code = 'GOV')
	and actor1_country_code = 'USA'
	and actor2_country_code = 'CHN'
	and file_id < 20150800
	group by date
) total on total.date = event.date
where  month_year = 201507
and is_root_event is true
and quad_class >= 3
and (goldstein_scale is null or goldstein_scale < 0)
and avg_tone < 0
and (actor1_type1_code = 'GOV'
or actor2_type1_code = 'GOV')
and actor1_country_code = 'USA'
and actor2_country_code = 'CHN'
and file_id < 20150800
group by total.date, total.events_nb, total.articles_nb, total.articles_avg







SELECT count(*) FROM gdelt-bq.gdeltv2.events
where MonthYear = 201502
and IsRootEvent = 1
and QuadClass >= 3
and GoldsteinScale < 0
and AvgTone < 0
and Actor1Type1Code = 'GOV'
and Actor2Type1Code = 'GOV'
and Actor1CountryCode = 'USA'
and Actor2CountryCode = 'CHN'