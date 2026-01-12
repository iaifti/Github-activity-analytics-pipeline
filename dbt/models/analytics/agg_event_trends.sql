with events as (
    select * from {{ ref('stg_github_events') }}
),

hourly_trends as (
    select
        repo_name,
        event_type,
        date_trunc('hour', event_timestamp)::timestamp as event_hour,
        
        count(*) as event_count,
        count(distinct actor_login) as unique_users,
        
        -- Comparing to previous hour
        lag(count(*)) over (
            partition by repo_name, event_type 
            order by date_trunc('hour', event_timestamp)
        ) as prev_hour_count,
        
        
        count(*) - lag(count(*)) over (
            partition by repo_name, event_type 
            order by date_trunc('hour', event_timestamp)
        ) as over_hour_change
        
    from events
    group by repo_name, event_type, date_trunc('hour', event_timestamp)
)

select * from hourly_trends
order by repo_name, event_type, event_hour desc