--exec silver.load_silver

create or alter procedure  silver.load_silver as
begin


	print '>> truncating [silver].[crm_cust_info]'
	truncate table [silver].[crm_cust_info]
	print '>> inserting data into [silver].[crm_cust_info]'
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



	print ' >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	print '>> truncating [silver].[crm_prd_info]'
	truncate table [silver].[crm_prd_info]
	print '>> inserting data into [silver].[crm_prd_info]'
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


	print ' >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	print '>> truncating [silver].[crm_sales_details]'
	truncate table [silver].[crm_sales_details]
	print '>> inserting data into [silver].[crm_sales_details]'
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



	print ' >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	print '>> truncating [silver].[erp_cust_az12]'
	truncate table [silver].[erp_cust_az12]
	print '>> inserting data into [silver].[erp_cust_az12]'
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



	print ' >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	print '>> truncating [silver].[erp_loc_a101]'
	truncate table [silver].[erp_loc_a101]
	print '>> inserting data into [silver].[erp_loc_a101]'
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



	print ' >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
	print '>> truncating [erp_px_cat_g1v2]'
	truncate table [silver].[erp_px_cat_g1v2]
	print '>> inserting data into [silver].[erp_px_cat_g1v2]'
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


end
