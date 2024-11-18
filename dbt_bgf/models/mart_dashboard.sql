
{{config(materialized='table',)}}

SELECT

    id                                        AS game_id
  , name                                      AS game_name
  , year                                      AS year_published
  , min_players                               AS min_players
  , max_players                               AS max_players
  , playing_time                              AS game_time
  , avg_rating                                AS game_rating
  , categories                                AS game_category
  , mechanics                                 AS game_mechanics
  , themes                                    AS gmae_themes
  , Description                               AS game_description
  , NumWish                                   AS nr_wished_users
  , NumUserRatings                            AS nr_rated_users

-- FROM {{ source('XXX') }}
