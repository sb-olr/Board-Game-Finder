{{config(materialized='table')}}

WITH parsed_themes AS (
    SELECT
        id,
        theme
    FROM (
        SELECT
            id,
            SPLIT(themes, ', ') AS themes_array
        FROM {{ source('bgf_project_silver', 'boardgamefinder') }}
    ), UNNEST(themes_array) AS theme
)
SELECT
    id as game_id,
    TRIM(theme) AS game_theme
FROM
    parsed_themes
WHERE theme IS NOT NULL
