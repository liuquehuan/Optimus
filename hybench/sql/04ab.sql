select custid, name,phone,sum(amount)
from (select cu.custid,cu.name,cu.phone,t.amount
from customer cu join transfer t on cu.custid = t.sourceid where custid= 67156
union all
select cu.custid,cu.name,cu.phone,ch.amount
from customer cu join checking ch on cu.custid = ch.sourceid where cu.custid= 67156) outcome
group by custid,name,phone;