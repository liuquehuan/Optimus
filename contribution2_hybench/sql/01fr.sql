select sourceid,targetid,
case when sourceid= 218778 then 'outbound' when targetid= 218778 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 218778 or targetid = 218778
group by sourceid,targetid
order by total_amount desc;