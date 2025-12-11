with base as (

    select *
    from {{ ref('int_merged_library') }}

),

kpi as (

    select
        -- PK propagée
        yt_track_id,
        
        -- Info de base utile en viz
        spotify_track_id,
        title,
        artist,
        album,
        main_genre,
        sub_genre,
        genres,

        -- on garde la version brute
        release_year,

        -- version castée en entier (si possible)
        safe_cast(release_year as int64) as release_year_int,

        duration_seconds,
        popularity,
        ytm_url,
        extraction_date,

        -- Âge du titre (en années)
        case 
            when safe_cast(release_year as int64) is null then null
            else extract(year from current_date()) - safe_cast(release_year as int64)
        end as track_age_years,

        -- Décennie de sortie (ex: 1990, 2000, 2010)
        case 
            when safe_cast(release_year as int64) is null then null
            else floor(safe_cast(release_year as int64) / 10) * 10
        end as release_decade,

        -- Bandes de popularité
        case
            when popularity is null      then 'Unknown'
            when popularity < 20         then '0-19'
            when popularity < 40         then '20-39'
            when popularity < 60         then '40-59'
            when popularity < 80         then '60-79'
            else                              '80-100'
        end as popularity_band,

        -- Durées dérivées
        duration_seconds / 60.0   as duration_minutes,
        duration_seconds / 3600.0 as duration_hours,

        case
            when duration_seconds is null       then 'Unknown'
            when duration_seconds < 150         then 'Short (<2:30)'
            when duration_seconds < 300         then 'Medium (2:30–5:00)'
            else                                     'Long (>5:00)'
        end as track_length_bucket,

        -- Flags qualité / enrichissement
        spotify_track_id is not null as has_spotify_match,
        main_genre      is not null as has_genre,
        ytm_url         is not null as has_ytm_url

    from base
)

select *
from kpi
