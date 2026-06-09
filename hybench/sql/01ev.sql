select sourceid,targetid,
case when sourceid= 145703 then 'outbound' when targetid= 145703 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 145703 or targetid = 145703
group by sourceid,targetid
order by total_amount desc;