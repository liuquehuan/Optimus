select sourceid,targetid,
case when sourceid= 11575545 then 'outbound' when targetid= 11575545 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 11575545 or targetid = 11575545
group by sourceid,targetid
order by total_amount desc;