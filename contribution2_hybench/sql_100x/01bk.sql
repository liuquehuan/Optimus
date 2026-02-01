select sourceid,targetid,
case when sourceid= 5416839 then 'outbound' when targetid= 5416839 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 5416839 or targetid = 5416839
group by sourceid,targetid
order by total_amount desc;