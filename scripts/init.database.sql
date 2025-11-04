use master;
go

if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse
end;
go

create database DataWarehouse;

use DataWarehouse;

create Schema bronze;
go
create Schema silver;
go
create Schema gold;
go
