select sourceid,targetid,
case when sourceid= 199715 then 'outbound' when targetid= 199715 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 199715 or targetid = 199715
group by sourceid,targetid
order by total_amount desc;