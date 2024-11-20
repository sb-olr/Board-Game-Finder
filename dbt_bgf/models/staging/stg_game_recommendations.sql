
{{config(materialized='table')}}

SELECT
    id AS game_id,
    ComMinPlaytime AS community_min_playtime,
    ComMaxPlaytime AS community_max_playtime,
    CAST(ComAgeRec AS INT64) AS community_age_reco,
    MfgAgeRec AS manufacturer_age_reco,
    MfgPlaytime AS manufacturer_reco_playtime
FROM
{{ source('bgf_project_silver', 'boardgamefinder') }}

