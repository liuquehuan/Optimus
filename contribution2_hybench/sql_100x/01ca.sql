select sourceid,targetid,
case when sourceid= 17567785 then 'outbound' when targetid= 17567785 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17567785 or targetid = 17567785
group by sourceid,targetid
order by total_amount desc;