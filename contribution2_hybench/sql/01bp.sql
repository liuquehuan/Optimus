select sourceid,targetid,
case when sourceid= 194434 then 'outbound' when targetid= 194434 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 194434 or targetid = 194434
group by sourceid,targetid
order by total_amount desc;