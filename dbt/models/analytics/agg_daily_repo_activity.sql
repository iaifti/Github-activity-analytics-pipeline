with events as(
    select * from {{ ref('stg_github_events') }}
),

activity as(
    select
        repo_name,
        date_trunc('day', event_timestamp)::date as activity_date,

        count(*) as total_activity,
        count(distinct actor_login) as unique_contributors,

        sum(case when event_type = 'PushEvent' then 1 else 0 end) as push_events,
        sum(case when event_type = 'PushEvent' then push_commit_count else 0 end) as total_commits,
        sum(case when event_type = 'IssuesEvent' then 1 else 0 end) as issue_events,
        sum(case when event_type = 'PullRequestEvent' then 1 else 0 end) as pr_events,
        sum(is_star_event) as star_events,
        sum(case when event_type = 'ForkEvent' then 1 else 0 end) as fork_events,

        avg(count(*)) over (
                partition by repo_name 
                order by date_trunc('day', event_timestamp)::date
                rows between 6 preceding and current row
        ) as week_avg,

    from events
    group by repo_name, date_trunc('day', event_timestamp)::date
)
select * from activity
order by repo_name, activity_date