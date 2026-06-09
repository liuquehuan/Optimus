select sourceid,targetid,
case when sourceid= 82249 then 'outbound' when targetid= 82249 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 82249 or targetid = 82249
group by sourceid,targetid
order by total_amount desc;