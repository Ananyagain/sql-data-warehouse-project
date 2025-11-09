select * from [silver].[crm_cust_info]
--select * from [bronze].[erp_cust_az12]


--STANDARDIZATION
select distinct gen,
case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
	  when upper(trim(gen)) in ('M', 'MALE') then 'Male'
	else 'N/A' end as gen from [bronze].[erp_cust_az12]


--checking
select 
case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
	end as cid
,bdate
,gen
from [bronze].[erp_cust_az12]
where case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid end not in (select cst_key from [silver].[crm_cust_info])


select bdate from [bronze].[erp_cust_az12] where bdate<'1924-01-01' or bdate>getdate()



---INSERTING INTO [silver].[erp_cust_az12]
INSERT INTO [silver].[erp_cust_az12](
cid
,bdate
,gen
)
select 
case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
	end as cid
,case when bdate>getdate() then null
	else bdate end as bdate
,case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
	  when upper(trim(gen)) in ('M', 'MALE') then 'Male'
	else 'N/A' end as gen		
from [bronze].[erp_cust_az12]

