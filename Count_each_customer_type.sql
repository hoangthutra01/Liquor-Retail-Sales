--Need to count the number of new and lost customers each month.
--Define new customers and lost customers:
---New customers are the ones who have the first order. They are considered as a new customer in that month.
---Lost customers are the ones who have no orders in EXACTLY 3 consecutive months.

with order_quantity_month as
(select 
  store_number 
  ,date_trunc (order_date, month) as order_month
  ,count (invoice_and_item_number) as count_lines
from `tra-lam-data.public_data_cleanse.Iowa_liquor_sales_cleanced`
where vendor_number=260
group by 1,2
order by 1,2
)

,store_data as
(select distinct (store_number)
from order_quantity_month
)

,month_data as
(select distinct (order_month)
from order_quantity_month
)

,store_by_month_densed as
(select 
  store_data.store_number
  ,month_data.order_month
  ,coalesce(order_quantity_month.count_lines,0) as count_lines
  ,store.first_order_date
  ,store.last_order_date
from store_data
cross join month_data
left join order_quantity_month using (store_number,order_month)
left join `tra-lam-data.public_data_cleanse.diageo_america_range_date`
  as store
  using(store_number)
where month_data.order_month between store.first_order_date and store.last_order_date + interval 3 month
--month_data.order_month needs to be added 3 months compared to the last order date of this store because the condition for a store to be recorded in "lost" status is 3 consecutive months without placing any orders
order by 1,2
)

,calculate_L3M as
(select 
  *
  ,sum (count_lines) over (partition by store_number order by order_month rows between 2 preceding and current row) as count_lines_L3M
  ,sum (count_lines) over (partition by store_number order by order_month rows between 3 preceding and 1 preceding) as count_lines_L4M
from store_by_month_densed
)

,store_by_month_final as
(select
  * except (first_order_date,last_order_date)
  ,case
    when first_order_date=order_month then 'New'
    when count_lines_L4M=0 and count_lines_L3M<>0 then 'Returning'
	--A store is in Returning state when it was in Lost state in the previous month, but has orders this month.
 	--count_lines_L4M=0 is equivalent to being in "Lost" status in the previous month, count_lines_L3M<>0 is equivalent to having orders in this month when count_lines_L4M=0 and count_lines_L3M=0 then 'Inactive'
	--A store is in Inactive status when last month was in Lost status and there are no orders this month.    when count_lines_L3M>0 then 'Existing'
    when count_lines_L3M=0 then 'Lost'
    else 'Undefined'
  end as customer_status
from calculate_L3M
order by 1,2
)

,count_full_year as
(select
  order_month
  ,count (case when customer_status = 'New' then store_number end) as count_new_customer
  ,count (case when customer_status = 'Lost' then store_number end) as count_lost_customer
  ,count (case when customer_status = 'Returning' then store_number end) as count_returning_customer
  ,count (case when customer_status not in ('Lost','Inactive')  then store_number end) as count_active_customer
from store_by_month_final
group by 1
order by 1
)

select *
from count_full_year
where order_month between '2021-01-01' and '2022-12-31'