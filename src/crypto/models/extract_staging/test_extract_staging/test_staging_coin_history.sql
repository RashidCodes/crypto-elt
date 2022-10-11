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
	image:: json ->> 'thumb' as thumbnail,
	market_data:: json -> 'current_price' ->> 'aud' as current_price,
	market_data:: json -> 'market_cap' ->> 'aud' as market_cap,
	market_data:: json -> 'total_volume' ->> 'aud' as total_volume,
	community_data:: json ->> 'facebook_likes' as facebook_likes,
	community_data:: json ->> 'twitter_followers' as twitter_followers,
	community_data:: json ->> 'reddit_average_posts_48h' as redit_average_posts_48h,
	community_data:: json ->> 'reddit_average_comments_48h' as reddit_average_comments_48h,
	community_data:: json ->> 'reddit_subscribers' as reddit_subscribers,
	community_data:: json ->> 'reddit_accounts_active_48h' as reddit_accounts_active_48h,
	developer_data:: json ->> 'forks' as forks,
	developer_data:: json ->> 'stars' as stars,
	developer_data:: json ->> 'subscribers' as subscribers,
	developer_data:: json ->> 'total_issues' as total_issues,
	developer_data:: json ->> 'closed_issues' as closed_issues,
	developer_data:: json ->> 'pull_requests_merged' as pull_requests_merged,
	developer_data:: json ->> 'pull_request_contributors' as pull_request_contributors,
	developer_data:: json -> 'code_additions_deletions_4_weeks' ->> 'additions' as code_additions_4_weeks,
	developer_data:: json -> 'code_additions_deletions_4_weeks' ->> 'deletions' as code_deletions_4_weeks,
	developer_data:: json ->> 'commit_count_4_weeks' as commit_count_4_weeks,
	public_interest_stats:: json ->> 'alexa_rank' as alexa_rank,
	public_interest_stats:: json ->> 'bing_matches' as bing_matches,
	price_date as last_updated_timestamp,
	now() as loaded_at

from {{ source_table_name }}

{% if is_incremental %}
    where price_date > '{{ incremental_value }}'
{% endif %}