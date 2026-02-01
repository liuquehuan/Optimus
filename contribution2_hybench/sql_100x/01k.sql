select sourceid,targetid,
case when sourceid= 22220228 then 'outbound' when targetid= 22220228 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 22220228 or targetid = 22220228
group by sourceid,targetid
order by total_amount desc;