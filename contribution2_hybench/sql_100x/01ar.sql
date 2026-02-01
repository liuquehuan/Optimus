select sourceid,targetid,
case when sourceid= 26866750 then 'outbound' when targetid= 26866750 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 26866750 or targetid = 26866750
group by sourceid,targetid
order by total_amount desc;