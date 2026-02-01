select sourceid,targetid,
case when sourceid= 3529345 then 'outbound' when targetid= 3529345 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 3529345 or targetid = 3529345
group by sourceid,targetid
order by total_amount desc;