select sourceid,targetid,
case when sourceid= 2845785 then 'outbound' when targetid= 2845785 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2845785 or targetid = 2845785
group by sourceid,targetid
order by total_amount desc;