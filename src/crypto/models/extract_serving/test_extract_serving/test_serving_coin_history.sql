{% set config = {
    "extract_type": "incremental", 
    "incremental_column": "last_updated_timestamp",
    "key_columns":["id", "last_updated_timestamp", "key"]
} %}


select 
	id,
	key, 
	symbol,
	name,
	current_price,
	market_cap,
	total_volume,
	twitter_followers,
	reddit_subscribers,
	forks,
	stars,
	closed_issues,
	pull_requests_merged,
	last_updated_timestamp,
	now() as loaded_at

from {{ source_table_name }}

{% if is_incremental %}
    where last_updated_timestamp > '{{ incremental_value }}'
{% endif %}
