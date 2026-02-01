select sourceid,targetid,
case when sourceid= 1505085 then 'outbound' when targetid= 1505085 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1505085 or targetid = 1505085
group by sourceid,targetid
order by total_amount desc;