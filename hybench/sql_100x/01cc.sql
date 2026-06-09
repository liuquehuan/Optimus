select sourceid,targetid,
case when sourceid= 1716733 then 'outbound' when targetid= 1716733 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1716733 or targetid = 1716733
group by sourceid,targetid
order by total_amount desc;