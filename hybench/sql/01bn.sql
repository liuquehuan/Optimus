select sourceid,targetid,
case when sourceid= 39163 then 'outbound' when targetid= 39163 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 39163 or targetid = 39163
group by sourceid,targetid
order by total_amount desc;