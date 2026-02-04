select sourceid,targetid,
case when sourceid= 27376851 then 'outbound' when targetid= 27376851 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 27376851 or targetid = 27376851
group by sourceid,targetid
order by total_amount desc;