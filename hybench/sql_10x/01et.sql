select sourceid,targetid,
case when sourceid= 439851 then 'outbound' when targetid= 439851 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 439851 or targetid = 439851
group by sourceid,targetid
order by total_amount desc;