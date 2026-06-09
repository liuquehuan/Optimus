select cid, total_expense, sum(ch.amount) as checking_amount from
(select co.companyid as cid, sum(tr.amount) as total_expense
from company co, transfer tr
where co.companyid=tr.sourceid and co.name like 'software%'
group by co.companyid
order by total_expense DESC) big_company, checking ch
where big_company.cid=ch.sourceid
group by cid, total_expense
order by total_expense DESC, checking_amount desc;