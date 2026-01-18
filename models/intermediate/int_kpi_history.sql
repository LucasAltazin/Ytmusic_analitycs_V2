-- models/intermediate/int_kpi_history.sql

select
    listen_date,

    -- core volume KPIs
    sum(listen_count)                               as total_listens,
    count(distinct spotify_track_id)                as unique_tracks,

    -- time KPIs
    sum(duration_minutes)                           as total_minutes,
    sum(duration_hours)                             as total_hours,

    -- averages
    safe_divide(
        sum(listen_count),
        count(distinct spotify_track_id)
    )                                               as avg_listens_per_track,

    -- peak listening hour per day
    array_agg(listen_hour order by listen_count desc limit 1)[offset(0)] as peak_listening_hour,

    -- seasonality
    extract(month from listen_date)                 as listen_month,
    extract(dayofweek from listen_date)            as listen_weekday

from {{ ref('int_kpi_core_history') }}
group by listen_date
order by listen_date
