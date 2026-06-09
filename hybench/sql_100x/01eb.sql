select sourceid,targetid,
case when sourceid= 801819 then 'outbound' when targetid= 801819 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 801819 or targetid = 801819
group by sourceid,targetid
order by total_amount desc;