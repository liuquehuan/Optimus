select sourceid,targetid,
case when sourceid= 2906994 then 'outbound' when targetid= 2906994 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2906994 or targetid = 2906994
group by sourceid,targetid
order by total_amount desc;