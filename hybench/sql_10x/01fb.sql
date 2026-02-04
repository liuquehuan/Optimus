select sourceid,targetid,
case when sourceid= 2856748 then 'outbound' when targetid= 2856748 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2856748 or targetid = 2856748
group by sourceid,targetid
order by total_amount desc;