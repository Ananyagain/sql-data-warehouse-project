select * from [bronze].[erp_loc_a101]
select * from [silver].[crm_cust_info]


--inserting
insert into [silver].[erp_loc_a101](
cid
,cntry
)
select 
replace(cid, '-', '') as cid
,case when upper(trim(cntry)) in ('USA', 'US') then 'United States'
	when upper(trim(cntry))  = 'DE' then 'Germany'
	when trim(cntry) = ' ' or trim(cntry) is null then 'N/A'
	else trim(cntry) end as cntry
from [bronze].[erp_loc_a101]

select * from [silver].[erp_loc_a101]




select distinct cntry 
,case when upper(trim(cntry)) in ('USA', 'US') then 'United States'
	when upper(trim(cntry))  = 'DE' then 'Germany'
	when trim(cntry) = ' ' or trim(cntry) is null then 'N/A'
	else trim(cntry) end as cntry
from [bronze].[erp_loc_a101]

--checking
select 
cid
,replace(cid, '-', '') as cid
,cntry
from [bronze].[erp_loc_a101]
where replace(cid, '-', '') not in (select cst_key from [silver].[crm_cust_info])
