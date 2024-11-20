{{config(materialized='table')}}

WITH parsed_categories AS (
    SELECT
        id,
        category
    FROM (
        SELECT
            id,
            SPLIT(categories, ', ') AS categories_array
        FROM {{ source('bgf_project_silver', 'boardgamefinder') }}
    ), UNNEST(categories_array) AS category
)
SELECT
    id as game_id,
    TRIM(category) AS game_category
FROM
    parsed_categories
WHERE category IS NOT NULL
