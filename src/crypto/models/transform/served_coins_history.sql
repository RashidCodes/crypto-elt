drop table if exists {{ target_table_name }};

create table {{ target_table_name }} as 
	with cleaned_coin_history as (
		select 
			id as coin_name,
			key as surrogate_key,
			symbol,
			name as other_name,
			coalesce(round(cast(current_price as numeric), 2), 0) as current_price,
			coalesce(round(cast(market_cap as numeric), 2), 0) as market_cap,
			coalesce(round(cast(total_volume as numeric), 2), 0) as total_volume,
			coalesce(twitter_followers::int, 0) as twitter_followers,
			coalesce(reddit_subscribers::int, 0) as reddit_subscribers,
			coalesce(forks::int, 0) as forks,
			coalesce(stars::int, 0) as github_stars,
			coalesce(closed_issues::int, 0) as closed_issues,
			coalesce(pull_requests_merged:: int, 0) as pull_requests_merged,
			last_updated_timestamp,
			loaded_at 
		from {{ source_table_name }}
	) 
	select *,
		(twitter_followers + reddit_subscribers) as social_media_followers,
		max(current_price) over (
			partition by coin_name, last_updated_timestamp 
			order by last_updated_timestamp 
			rows between unbounded preceding 
				and unbounded following
		) as all_time_high
	from cleaned_coin_history;


	
	
	
		
		