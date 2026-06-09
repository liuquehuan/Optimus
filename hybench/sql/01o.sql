select sourceid,targetid,
case when sourceid= 225700 then 'outbound' when targetid= 225700 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 225700 or targetid = 225700
group by sourceid,targetid
order by total_amount desc;