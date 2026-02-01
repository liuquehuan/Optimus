select sourceid,targetid,
case when sourceid= 316861 then 'outbound' when targetid= 316861 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 316861 or targetid = 316861
group by sourceid,targetid
order by total_amount desc;