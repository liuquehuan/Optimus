select sourceid,targetid,
case when sourceid= 2351382 then 'outbound' when targetid= 2351382 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2351382 or targetid = 2351382
group by sourceid,targetid
order by total_amount desc;