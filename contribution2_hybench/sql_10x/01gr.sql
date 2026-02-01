select sourceid,targetid,
case when sourceid= 747973 then 'outbound' when targetid= 747973 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 747973 or targetid = 747973
group by sourceid,targetid
order by total_amount desc;