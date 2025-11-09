select prd_id,count(*) from [silver].[crm_prd_info]
group by prd_id
having count(*) >1 or prd_id is null

select * from bronze.crm_prd_info
select * from [bronze].[crm_sales_details]
select * from [bronze].[erp_px_cat_g1v2] --contains category id
select * from [bronze].[erp_loc_a101]
select * from [bronze].[erp_cust_az12]

select prd_nm from silver.crm_prd_info
where prd_nm != trim(prd_nm)

select prd_cost from silver.crm_prd_info where prd_cost is not null

select * from bronze.crm_prd_info where prd_line is null
select distinct prd_line from bronze.crm_prd_info

select * from bronze.crm_prd_info where prd_start_dt is null

select * from bronze.crm_prd_info where prd_end_dt is null


where replace(SUBSTRING(prd_key, 1, 5), '-', '_') not in (select distinct id from [bronze].[erp_px_cat_g1v2])

--no duplicates prd_id
--no nulls in prd_key, prd_nm
--no spcaes in prd_nm
--remove rocords with prd_cost is null
select prd_cost from bronze.crm_prd_info where prd_cost < 0 or prd_cost is null
--no product start date is null, start_date < end date

select * from silver.crm_prd_info where prd_end_dt<prd_start_dt --data issue

select * from bronze.crm_prd_info where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

select 
prd_id
,prd_key
,prd_nm
,prd_cost
,prd_line
,prd_start_dt
--,prd_end_dt
,lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_date_test
from bronze.crm_prd_info where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


--inserting to silver.crm_prd_info
insert into silver.crm_prd_info(
prd_id
,cat_id
,prd_key
,prd_nm
,prd_cost
,prd_line
,prd_start_dt
,prd_end_dt
)
select 
prd_id
,replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id --to join [erp_px_cat_g1v2]
,SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key  --to join [crm_sales_details]
,prd_nm
,isnull(prd_cost, 0) as prd_cost
,case upper(trim(prd_line))
	  when 'M' then 'Mountain'
	  when 'R' then 'Road'
	  when 'S' then 'Other Sales'
	  when 'T' then 'Touring'
	  else 'n/a'
end as prd_line
,cast(prd_start_dt as date) as prd_start_dt
,cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
from bronze.crm_prd_info



