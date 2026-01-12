with source as (
    select * from {{ source('raw', 'raw_github_events') }}
),

flattened as (
    select
        raw_data:id::varchar as event_id,
        raw_data:type::varchar as event_type,
        raw_data:actor.id::int as actor_id,
        raw_data:actor.login::varchar as actor_login,
        raw_data:repo.id::int as repo_id,
        raw_data:repo.name::varchar as repo_name,
        raw_data:created_at::timestamp as event_timestamp,
        raw_data:_extracted_at::timestamp as extracted_at,
        raw_data:_source_repo::varchar as source_repo,
        
        
        raw_data:payload as payload,
        
        case 
            when raw_data:type::varchar = 'PushEvent' 
            then raw_data:payload.size::int 
        end as push_commit_count,
        
        case 
            when raw_data:type::varchar = 'IssuesEvent' 
            then raw_data:payload.action::varchar 
        end as issue_action,
        
        case 
            when raw_data:type::varchar = 'PullRequestEvent' 
            then raw_data:payload.action::varchar 
        end as pr_action,
        
        case 
            when raw_data:type::varchar = 'WatchEvent' 
            then 1 
            else 0 
        end as is_star_event,
        
        loaded_at
        
    from source
)

select * from flattened