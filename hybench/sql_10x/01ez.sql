select sourceid,targetid,
case when sourceid= 1763188 then 'outbound' when targetid= 1763188 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1763188 or targetid = 1763188
group by sourceid,targetid
order by total_amount desc;