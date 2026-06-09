select sourceid,targetid,
case when sourceid= 1637851 then 'outbound' when targetid= 1637851 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1637851 or targetid = 1637851
group by sourceid,targetid
order by total_amount desc;