{% set config = {
    "extract_type": "full"
} %}

-- Coins: Transformation for staging 
-- SNAPSHOT ONLY
select 
	id,
	symbol,
	name,
	update_timestamp,
	now() as loaded_at 

from {{ source_table_name }}
