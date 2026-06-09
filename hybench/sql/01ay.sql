select sourceid,targetid,
case when sourceid= 264677 then 'outbound' when targetid= 264677 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 264677 or targetid = 264677
group by sourceid,targetid
order by total_amount desc;