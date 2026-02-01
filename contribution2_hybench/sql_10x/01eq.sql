select sourceid,targetid,
case when sourceid= 1389402 then 'outbound' when targetid= 1389402 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1389402 or targetid = 1389402
group by sourceid,targetid
order by total_amount desc;