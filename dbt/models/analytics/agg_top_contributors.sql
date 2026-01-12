with events as (

    select * from {{ ref('stg_github_events') }}
),

top_contributors as (

    select 
        actor_login as contributor,
        repo_name,

        count(*) as total_events,
        count(distinct event_type) as event_types_used,

        sum(case when event_type = 'PushEvent' then 1 else 0 end) as push_count,
        sum(case when event_type = 'PushEvent' then push_commit_count else 0 end) as commit_count,
        sum(case when event_type = 'IssuesEvent' then 1 else 0 end) as issue_count,
        sum(case when event_type = 'PullRequestEvent' then 1 else 0 end) as pr_count,

        min(event_timestamp) as first_commit,
        max(event_timestamp) as final_commit,

        datediff(day, min(event_timestamp) , max(event_timestamp)) as active_days,

        (sum(case when event_type = 'PushEvent' then push_commit_count else 0 end) * 2) +  -- Commits worth 2 points
        (sum(case when event_type = 'PullRequestEvent' then 1 else 0 end) * 5) +  -- PRs worth 5 points
        (sum(case when event_type = 'IssuesEvent' then 1 else 0 end) * 1) as contribution_score,
        
        
        rank() over (
            partition by repo_name 
            order by count(*) desc
        ) as contributor_rank
        
    from events
    group by actor_login, repo_name
)

select * from top_contributors
where contributor_rank <= 20
order by repo_name, contribution_score desc