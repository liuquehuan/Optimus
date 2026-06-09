select sourceid,targetid,
case when sourceid= 243906 then 'outbound' when targetid= 243906 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 243906 or targetid = 243906
group by sourceid,targetid
order by total_amount desc;