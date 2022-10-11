{% set config = {
    "extract_type": "full"
} %}

select 
	id,
	coin_id,
	name,
	symbol,
	market_cap_rank,
	price_btc, 
	score,
	update_timestamp, 
	now() as loaded_at
from {{ source_table_name }}