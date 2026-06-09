select sourceid,targetid,
case when sourceid= 795451 then 'outbound' when targetid= 795451 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 795451 or targetid = 795451
group by sourceid,targetid
order by total_amount desc;