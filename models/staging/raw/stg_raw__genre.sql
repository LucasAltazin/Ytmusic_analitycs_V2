with 

source as (

    select * from {{ source('raw', 'genre') }}

),

renamed as (

    select
        spotify_raw_genre,
        main_genre,
        sub_genre

    from source

)

select * from renamed