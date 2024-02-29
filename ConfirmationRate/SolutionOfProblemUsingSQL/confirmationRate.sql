with cte as (
    select user_id , time_stamp,
    case
    when action='confirmed' then 1 else 0
    end as confirmations 
    from Confirmations
)
select s.user_id,
case when c.user_id is null then round(0,2) else round(cast(coalesce(tot_confirmations,0)as float)/cast(coalesce(cnt_user,0) as float),2) end as confirmation_rate 
from signups s left join 
(select user_id, count(user_id) cnt_user, sum(confirmations) tot_confirmations from cte group by user_id) c
on s.user_id=c.user_id 