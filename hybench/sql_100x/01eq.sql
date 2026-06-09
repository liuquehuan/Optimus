select sourceid,targetid,
case when sourceid= 851175 then 'outbound' when targetid= 851175 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 851175 or targetid = 851175
group by sourceid,targetid
order by total_amount desc;