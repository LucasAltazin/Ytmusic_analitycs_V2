-- KPIs aggregated at ARTIST level
-- Grain: 1 row per artist

select
    artist,

    -- Core usage
    sum(listen_count)                            as total_listens,
    count(distinct listen_date)                  as active_days,

    -- Discovery / variety
    count(distinct spotify_track_id)             as unique_tracks,

    -- Time
    sum(duration_minutes)                        as total_minutes,
    sum(duration_hours)                          as total_hours,

    -- Normalised metrics
    safe_divide(
        sum(listen_count),
        count(distinct listen_date)
    )                                            as avg_listens_per_active_day

from {{ ref('int_kpi_core_history') }}

-- Safety: avoid null artist rows polluting rankings
where artist is not null

group by artist
order by total_listens desc
