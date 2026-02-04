select sourceid,targetid,
case when sourceid= 2967710 then 'outbound' when targetid= 2967710 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2967710 or targetid = 2967710
group by sourceid,targetid
order by total_amount desc;