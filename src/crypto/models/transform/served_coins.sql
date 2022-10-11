drop table if exists {{ target_table_name }};

create table as {{ target_table_name }} as 
	select 
		id as coin_name,
		symbol,
		name as other_name,
		update_timestamp as last_updated_timestamp,
		loaded_at 
	from {{ source_table_name }}