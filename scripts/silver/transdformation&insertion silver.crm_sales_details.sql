select * from [bronze].[crm_sales_details]
select * from [silver].[crm_prd_info]


select sls_ord_num,count(sls_ord_num) from  [bronze].[crm_sales_details]
group by sls_ord_num
having count(*)>1 or sls_ord_num is null



where sls_sales <=0 or sls_sales is null

where sls_cust_id not in (select cst_id from [silver].[crm_cust_info])

--check for invalid dates
select nullif(sls_order_dt,0) sls_order_dt --if 0 then make null
from bronze.crm_sales_details
where sls_order_dt <=0 or len(sls_order_dt) !=8

--check for invalid date orders
select *
from bronze.crm_sales_details
where sls_order_dt>sls_ship_dt or 
sls_order_dt>sls_due_dt or sls_ship_dt>sls_due_dt


--checking values
select distinct sls_sales, sls_quantity, sls_price
,case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) 
			then  sls_quantity * abs(sls_price)
	else sls_sales
	end as sls_sales_new

,case when sls_price <=0 or sls_price is null
		then sls_sales / nullif(sls_quantity, 0)
	else sls_price end as sls_price_new

, case when sls_quantity <=0 or sls_quantity is null
		then sls_sales / nullif(sls_price, 0)
	else sls_quantity end as sls_quantity_new
from bronze.crm_sales_details

where sls_sales != sls_quantity * sls_price 
or sls_sales <=0 or sls_sales is null
or sls_quantity <=0 or sls_quantity is null
or sls_price <=0 or sls_price is null
order by sls_sales, sls_quantity, sls_price

--sales = quantity*price, -ve, 0's, nulls not allowed




--inserting into silver.crm_sales_details
insert into [silver].[crm_sales_details](
sls_ord_num
,sls_prd_key
,sls_cust_id
,sls_order_dt
,sls_ship_dt
,sls_due_dt
,sls_sales
,sls_quantity
,sls_price
)
select 
sls_ord_num
,sls_prd_key
,sls_cust_id
,case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
	else cast(cast(sls_order_dt as varchar) as date) 
	end as sls_order_dt
,case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
	else cast(cast(sls_ship_dt as varchar) as date) 
	end as sls_ship_dt
,case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
	else cast(cast(sls_due_dt as varchar) as date) 
	end as sls_due_dt
,case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) 
			then  sls_quantity * abs(sls_price)
	else sls_sales
	end as sls_sales
,case when sls_quantity <=0 or sls_quantity is null
		then sls_sales / nullif(sls_price, 0)
	else sls_quantity end as sls_quantity
,case when sls_price <=0 or sls_price is null
		then sls_sales / nullif(sls_quantity, 0)
	else sls_price end as sls_price
from bronze.crm_sales_details


select * from [silver].[crm_sales_details]
where sls_quantity>1
