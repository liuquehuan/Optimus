select sourceid,targetid,
case when sourceid= 26524163 then 'outbound' when targetid= 26524163 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 26524163 or targetid = 26524163
group by sourceid,targetid
order by total_amount desc;