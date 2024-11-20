
{{config(materialized='table',)}}

SELECT

    CAST(id AS INT64)                 AS game_id,
    CAST(name AS STRING)              AS game_name,
    CAST(year AS INT64)               AS year_published,
    CAST(min_players AS INT64)        AS min_players,
    CAST(max_players AS INT64)        AS max_players,
    CAST(playing_time AS FLOAT64)     AS game_duration,
    CAST(min_playtime AS FLOAT64)     AS min_playing_time,
    CAST(max_playtime AS FLOAT64)     AS max_playing_time,
    CAST(min_age AS INT64)            AS min_age,
    CAST(description AS STRING)       AS game_description,
    CAST(ImagePath AS STRING)         AS image_path,
    CAST(`rank:boardgame` AS INT64)   AS game_rank

FROM
{{ source('bgf_project_silver', 'boardgamefinder') }}
