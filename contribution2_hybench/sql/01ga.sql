select sourceid,targetid,
case when sourceid= 4539 then 'outbound' when targetid= 4539 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 4539 or targetid = 4539
group by sourceid,targetid
order by total_amount desc;