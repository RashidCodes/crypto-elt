drop table if exists {{ target_table_name }};

create table as {{ target_table_name }} as 
	select 
		id as coin_name,
		symbol,
		coin_id,
		market_cap_rank,
		round(cast(price_btc as numeric), 2) as price_in_btc,
		score,
		update_timestamp as last_updated_timestamp,
		loaded_at 
	from {{ source_table_name }}