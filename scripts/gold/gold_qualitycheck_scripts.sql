select * from [silver].[crm_cust_info]
select * from [silver].[erp_cust_az12]
select * from [silver].[erp_loc_a101]

select cst_id, count(*) from( --to check if duplicate ids formed after joining
select 
ci.cst_id
,ci.cst_key
,ci.cst_firstname
,ci.cst_lastname
,ci.cst_marital_status
,ci.cst_gndr
,ci.cst_create_date
,ca.bdate
,ca.gen
,cl.cntry
from [silver].[crm_cust_info] ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join [silver].[erp_loc_a101] cl
on ci.cst_key=cl.cid)t
group by cst_id
having count(*)>1

select
ci.cst_gndr,--1
ca.gen,     --2
case when ci.cst_gndr != 'n/a' then ci.cst_gndr --crm is master for gender
	else coalesce(ca.gen, 'n/a') --else take value from erp and if null then make ot n/a
	end as new_gen
from [silver].[crm_cust_info] ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join [silver].[erp_loc_a101] cl
on ci.cst_key=cl.cid
order by 1,2

select * from gold.fact_sales f -- checking foreign key integration
left join gold.dem_customers c
on f.customer_key = c.customer_key
where c.customer_key is null
