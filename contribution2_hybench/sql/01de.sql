select sourceid,targetid,
case when sourceid= 286248 then 'outbound' when targetid= 286248 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 286248 or targetid = 286248
group by sourceid,targetid
order by total_amount desc;