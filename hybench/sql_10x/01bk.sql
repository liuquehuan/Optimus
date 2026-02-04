select sourceid,targetid,
case when sourceid= 1252172 then 'outbound' when targetid= 1252172 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1252172 or targetid = 1252172
group by sourceid,targetid
order by total_amount desc;