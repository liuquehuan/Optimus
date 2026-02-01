select sourceid,targetid,
case when sourceid= 11842985 then 'outbound' when targetid= 11842985 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 11842985 or targetid = 11842985
group by sourceid,targetid
order by total_amount desc;