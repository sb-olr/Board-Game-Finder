{{config(materialized='view')}}

SELECT
    g.game_id AS `Game ID`,
    g.game_name AS `Game Name`,
    g.year_published AS `Year Published`,
    g.min_players AS `Nr Players Min`,
    g.max_players,
    g.game_duration AS `Game Duration`,
    g.image_path,
    gr.avg_rating AS `Game Rating`,
    gr.num_ratings AS `Nr Ratings`,
    gr.num_owners AS `Nr Owners`,
    gr.num_comments AS `Nr Comments`,
    g.game_description AS `Description`,
    gm.game_mechanics AS `Select Mechanics`,
    gc.game_category AS `Select Category`

FROM
    {{ ref('stg_games') }} g
INNER JOIN
    {{ ref('stg_game_mechanics') }} gm ON g.game_id = gm.game_id
INNER JOIN
    {{ ref('stg_game_ratings') }} gr ON g.game_id = gr.game_id
INNER JOIN
    {{ ref('stg_game_category') }} gc ON g.game_id = gc.game_id
WHERE
    gr.num_ratings >= 100
GROUP BY
    g.game_id,
    g.game_name,
    g.year_published,
    g.min_players,
    g.max_players,
    g.game_duration,
    g.image_path,
    gr.avg_rating,
    gr.num_ratings,
    gr.num_owners,
    gr.num_comments,
    g.game_description,
    gm.game_mechanics,
    gc.game_category
