select sourceid,targetid,
case when sourceid= 17677917 then 'outbound' when targetid= 17677917 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17677917 or targetid = 17677917
group by sourceid,targetid
order by total_amount desc;