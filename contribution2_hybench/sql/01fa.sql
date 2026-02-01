select sourceid,targetid,
case when sourceid= 31859 then 'outbound' when targetid= 31859 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 31859 or targetid = 31859
group by sourceid,targetid
order by total_amount desc;