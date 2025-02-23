{{ 
    config(
        materialized='table',
        alias='event_details'
    ) 
}}

WITH raw_json AS (
    SELECT *, filename
    FROM read_json_objects('data/all_json/*.json', filename=true, format='auto')
)

SELECT 
    filename,
    json->'meta'->>'data_version' AS data_version,
    json->'meta'->>'created' AS created,
    json->'meta'->>'revision' AS revision,
    json->'info'->'event'->>'name' AS event_name,
    json->'info'->'event'->>'match_number' AS match_number,
    json->'info'->>'gender' AS gender,
    json->'info'->>'match_type' AS match_type,
    json->'info'->>'season' AS season,
    json->'info'->>'team_type' AS team_type,
    json->'info'->>'overs' AS overs
FROM raw_json