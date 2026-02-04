select sourceid,targetid,
case when sourceid= 1198712 then 'outbound' when targetid= 1198712 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1198712 or targetid = 1198712
group by sourceid,targetid
order by total_amount desc;