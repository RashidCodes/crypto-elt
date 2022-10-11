{% set config = {
    "extract_type": "full"
} %}

select 
	id,
	symbol,
	name,
	update_timestamp,
	now() as loaded_at
from {{ source_table_name }}