
{{config(materialized='table')}}

SELECT
    id AS game_id,
    weight,
    num_ratings,
    avg_rating,
    bayes_avg_rating,
    NumComments AS num_comments,
    NumWeightVotes AS num_weight_votes,
    num_owners
FROM
{{ source('bgf_project_silver', 'boardgamefinder') }}
WHERE id IS NOT NULL

