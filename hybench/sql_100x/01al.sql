select sourceid,targetid,
case when sourceid= 15835718 then 'outbound' when targetid= 15835718 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 15835718 or targetid = 15835718
group by sourceid,targetid
order by total_amount desc;