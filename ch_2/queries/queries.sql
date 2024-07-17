-- How many users were active on a given day (they made a deposit or withdrawal)
select count(DISTINCT (user_id))
from Transactions
where DATE(event_timestamp) = '2024-07-13'; -- replace qith the desired date



-- Identify users haven't made a deposit
with users_with_transactions_cte as 
(
    select DISTINCT (user_id)
    from Transactions
    where transaction_type = 'deposit'
)

select user_id
from Users
where user_id not in users_with_transactions_cte;



-- Identify on a given day which users have made more than 5 deposits historically
select user_id, 
    transaction_type, 
    DATE(event_timestamp) as event_date , 
    count(user_id, transaction_type, DATE(event_timestamp)) as transactions_per_day
from  Transactions
group by user_id, transaction_type, DATE(event_timestamp)
having transaction_type ='deposit' and transactions_per_day >= 5 and event_date = '2024-07-13' -- desired date;


-- When was the last time a user made a login
select user_id, MAX(event_timestamp) as last_login
from Events
where event_name like '%login%'
group by user_id;

select user_id, count(user_id) as login_count
from Events
where event_name like '%login%'
    and DATE(event_timestamp) between '2024-06-13' and '2024-01-13' -- desired dates
group by user_id;


-- Number of unique currencies deposited on a given day
select count ( DISTINCT currency) 
from Transactions
where transaction_type = 'deposit' 
    and DATE(event_timestamp) = '2024-07-13'; -- use the desired date


-- Number of unique currencies withdrew on a given day
select count ( DISTINCT currency) 
from Transactions
where transaction_type = 'withdrawal' 
    and DATE(event_timestamp) = '2024-07-13'; -- use the desired date


-- Total amount deposited of a given currency on a given day
select sum(amount)
from Transactions
where transaction_type = 'deposit'
    and currency = 'btc'
    and DATE(event_timestamp) = '2024-07-13'

