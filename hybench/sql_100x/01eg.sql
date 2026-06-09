select sourceid,targetid,
case when sourceid= 2916097 then 'outbound' when targetid= 2916097 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2916097 or targetid = 2916097
group by sourceid,targetid
order by total_amount desc;