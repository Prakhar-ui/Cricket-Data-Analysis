{{
  config(
    materialized = "table",
  )
}}

with source as (
    select
        filename,
        filedate,
        json_data -> 'info' ->> 'city' AS city,
        json_data -> 'info' -> 'toss' ->> 'uncontested' AS toss_uncontested,
        json_data -> 'info' -> 'toss' ->> 'winner' AS toss_winner,
        json_data -> 'info' -> 'toss' ->> 'decision' AS toss_decision,
        json_data -> 'info' ->> 'dates' AS dates,
        json_data -> 'info' -> 'event' ->> 'name' AS event_name,
        json_data -> 'info' -> 'event' ->> 'match_number' AS event_match_number,
        json_data -> 'info' -> 'event' ->> 'group' AS event_group,
        json_data -> 'info' -> 'event' ->> 'stage' AS event_stage,
        json_data -> 'info' ->> 'overs' AS overs,
        json_data -> 'info' ->> 'teams' AS teams,
        json_data -> 'info' ->> 'venue' AS venue,
        json_data -> 'info' ->> 'gender' AS gender,
        json_data -> 'info' ->> 'season' AS season,
        json_data -> 'info' -> 'outcome' -> 'by' ->> 'wickets'  AS outcome_by_wickets,
        json_data -> 'info' -> 'outcome' -> 'by' ->> 'runs'  AS outcome_by_runs,
        json_data -> 'info' -> 'outcome' -> 'by' ->> 'innings'  AS outcome_by_innings,
        json_data -> 'info' -> 'outcome' -> 'bowl_out'  AS outcome_bowl_out,
        json_data -> 'info' -> 'outcome' -> 'eliminator'  AS outcome_eliminator,
        json_data -> 'info' -> 'outcome' -> 'method'  AS outcome_method,
        json_data -> 'info' -> 'outcome' -> 'result'  AS outcome_result,
        json_data -> 'info' -> 'outcome' -> 'winner'  AS match_winner,
        json_data -> 'info' -> 'players'  AS players,
        json_data -> 'info' -> 'registry'  AS registry,
        json_data -> 'info' -> 'officials' ->> 'tv_umpires'  AS tv_umpires,
        json_data -> 'info' -> 'officials' ->> 'match_referees'  AS match_referees,
        json_data -> 'info' -> 'officials' ->> 'umpires'  AS umpires,
        json_data -> 'info' -> 'officials' ->> 'reserve_umpires'  AS reserve_umpires,
        json_data -> 'info' -> 'team_type'  AS team_type,
        json_data -> 'info' -> 'match_type'  AS match_type,
        json_data -> 'info' -> 'balls_per_over'  AS balls_per_over,
        json_data -> 'info' -> 'player_of_match'  AS player_of_match,
        json_data -> 'meta' -> 'created' AS meta_created,
        json_data -> 'meta' -> 'revision' AS meta_revision,
        json_data -> 'meta' -> 'data_version' AS meta_data_version,
        json_data -> 'info' -> 'missing' AS missing,
        json_data -> 'info' -> 'supersubs' AS supersubs,
        json_data -> 'info' -> 'match_type_number' AS match_type_number,
        json_data -> 'info' -> 'bowl_out' ->> 'bowler' AS bowl_out_bowler,
        json_data -> 'info' -> 'bowl_out' ->> 'outcome' AS bowl_out_outcome,
    from {{ source('landing_zone', 'all_match_data') }}
),

select * from source;

