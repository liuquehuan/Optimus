select sourceid,targetid,
case when sourceid= 964788 then 'outbound' when targetid= 964788 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 964788 or targetid = 964788
group by sourceid,targetid
order by total_amount desc;