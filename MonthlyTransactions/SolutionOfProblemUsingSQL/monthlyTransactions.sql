with cte as
(select id,
substring(cast(trans_date as varchar),1,7) as month,
country,
amount,
case when state='approved' then 1 else 0 end as approved_ind,
case when state='approved' then amount else 0 end as approved_amt from Transactions)
select month,
country , 
count(id) trans_count,
sum(approved_ind) approved_count,
sum(amount) trans_total_amount,
sum(approved_amt) approved_total_amount 
from cte
group by month ,country