select sourceid,targetid,
case when sourceid= 9867190 then 'outbound' when targetid= 9867190 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 9867190 or targetid = 9867190
group by sourceid,targetid
order by total_amount desc;