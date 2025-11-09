select * from [bronze].[erp_px_cat_g1v2]
select * from [silver].[crm_prd_info]
select * from [silver].[erp_px_cat_g1v2]

select 
maintenance 
from [bronze].[erp_px_cat_g1v2]
where maintenance != trim(maintenance)

--insertion 
insert into [silver].[erp_px_cat_g1v2](
id
,cat
,subcat
,maintenance
)
select 
id
,cat
,subcat
,maintenance
from [bronze].[erp_px_cat_g1v2]





--insertion if table does not exist
SELECT *
INTO silver.erp_px_cat_g1v2
FROM bronze.erp_px_cat_g1v2;

--insertion when schema exists
insert into silver.erp_px_cat_g1v2
select * from bronze.erp_px_cat_g1v2;
