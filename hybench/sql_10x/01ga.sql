select sourceid,targetid,
case when sourceid= 144849 then 'outbound' when targetid= 144849 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 144849 or targetid = 144849
group by sourceid,targetid
order by total_amount desc;