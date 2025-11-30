--select * from gold.dem_customers

create view gold.dem_customers as
--dimention customer (dimention table)
select 
row_number() over(order by cst_id) as customer_key,
ci.cst_id as customer_id
,ci.cst_key as customer_number
,ci.cst_firstname as firstname
,ci.cst_lastname as lastname
,cl.cntry as country
,ci.cst_marital_status as marital_status
,case when ci.cst_gndr != 'n/a' then ci.cst_gndr --crm is master for gender
	else coalesce(ca.gen, 'n/a') --else take value from erp and if null then make ot n/a
	end as gender
,ca.bdate as birthdate
,ci.cst_create_date as create_date
from [silver].[crm_cust_info] ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join [silver].[erp_loc_a101] cl
on ci.cst_key=cl.cid

--surrogate keys: system generated unique uidentifier assigned to each rocord in a table, only used to connect the tables, not used in business


create view gold.dem_products as
select 
row_number() over(order by cp.prd_start_dt, cp.prd_key) as product_key
,cp.prd_id as product_id
,cp.prd_key as product_number
,cp.prd_nm as product_name
,cp.cat_id as category_id
,ep.cat as category
,ep.subcat as subcategory
,ep.maintenance as maintenance
,cp.prd_cost as product_cost
,cp.prd_line as product_line
,cp.prd_start_dt as product_startdate
from [silver].[crm_prd_info] cp
left join [silver].[erp_px_cat_g1v2] ep
on cp.cat_id = ep.id
where cp.prd_end_dt is null --only current products


create view gold.fact_sales as
select 
cs.sls_ord_num as order_number
,p.product_key--surrogate key
,c.customer_key--surrogate key
,cs.sls_order_dt as order_date
,cs.sls_ship_dt as shipping_date
,cs.sls_due_dt as due_date
,cs.sls_sales as sales
,cs.sls_quantity as aquantity
,cs.sls_price as price
from [silver].[crm_sales_details] cs
left join gold.dem_products p
on cs.sls_prd_key = p.product_number
left join gold.dem_customers c
on cs.sls_cust_id = c.customer_id
