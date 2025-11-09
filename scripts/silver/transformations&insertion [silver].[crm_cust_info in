select * from bronze.crm_cust_info where cst_id  = 29449
--to find duplicate primary keys
select * from (
select *,
row_number() over(partition by cst_id order by cst_create_date desc) rn 
from bronze.crm_cust_info)t where rn =1 


--delete from rnk_cte
--where rn>1

--check for unwanted spaces
select cst_lastname from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname) --cst_firstname

--data sandardization and consistency
select distinct cst_gndr from bronze.crm_cust_info


--inserting into silver layer
insert into [silver].[crm_cust_info](
cst_id
,cst_key
,cst_firstname
,cst_lastname
,cst_marital_status
,cst_gndr
,cst_create_date
)
select 
cst_id
,cst_key
,trim(cst_firstname) as cst_firstname
,trim(cst_lastname) as cst_lastname
,case when trim(upper(cst_marital_status)) = 'M' then 'Married'
	  when trim(upper(cst_marital_status)) = 'S' then 'Single'
	  else 'N/A'
end as cst_marital_status --normalize marital_status to readable format
,case when trim(upper(cst_gndr)) = 'M' then 'Male'
	  when trim(upper(cst_gndr)) = 'F' then 'Female'
	  else 'N/A'
end as cst_gndr --normalize gender to readable format
,cst_create_date 
from (select *,
row_number() over(partition by cst_id order by cst_create_date desc) rn 
from bronze.crm_cust_info where cst_id is not null )t where rn = 1







