select sourceid,targetid,
case when sourceid= 2734521 then 'outbound' when targetid= 2734521 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2734521 or targetid = 2734521
group by sourceid,targetid
order by total_amount desc;