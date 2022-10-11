{% set config = {
    "extract_type": "full"
} %}

-- Trending: Transformation for staging 
-- SNAPSHOT ONLY
select 
	id,
	coin_id,
	name,
	symbol,
	market_cap_rank,
	thumb,
	small,
	large,
	slug,
	price_btc,
	score,
	update_timestamp,
	now() as loaded_at

from {{ source_table_name }}