select sourceid,targetid,
case when sourceid= 2689118 then 'outbound' when targetid= 2689118 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2689118 or targetid = 2689118
group by sourceid,targetid
order by total_amount desc;