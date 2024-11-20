{{config(materialized='table')}}

WITH parsed_mechanics AS (
    SELECT
        id,
        mechanics
    FROM (
        SELECT
            id,
            SPLIT(mechanics, ', ') AS mechanics_array
        FROM {{ source('bgf_project_silver', 'boardgamefinder') }}
    ), UNNEST(mechanics_array) AS mechanics
)
SELECT
    id as game_id,
    TRIM(mechanics) AS game_mechanics
FROM
    parsed_mechanics
WHERE mechanics IS NOT NULL
