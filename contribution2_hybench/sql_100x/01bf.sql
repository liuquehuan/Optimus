select sourceid,targetid,
case when sourceid= 19037155 then 'outbound' when targetid= 19037155 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 19037155 or targetid = 19037155
group by sourceid,targetid
order by total_amount desc;