select sourceid,targetid,
case when sourceid= 37797 then 'outbound' when targetid= 37797 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 37797 or targetid = 37797
group by sourceid,targetid
order by total_amount desc;