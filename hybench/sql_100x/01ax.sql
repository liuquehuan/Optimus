select sourceid,targetid,
case when sourceid= 11581745 then 'outbound' when targetid= 11581745 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 11581745 or targetid = 11581745
group by sourceid,targetid
order by total_amount desc;