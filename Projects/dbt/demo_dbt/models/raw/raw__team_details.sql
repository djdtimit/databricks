{{ config(
    tags = ["special"]
) }}

WITH teams AS (

    SELECT
        *
    FROM
        {{ ref('teams') }}
),
team_locations AS (
    SELECT
        *
    FROM
        {{ ref('team_locations') }}
),
FINAL AS (
    SELECT
        teams.team,
        TRIM(
            team_locations.city
        ) AS city,
        TRIM(
            team_locations.state
        ) AS state,
        teams.team = '{{ var("current_champion") }}' AS is_champion
    FROM
        teams
        LEFT JOIN team_locations
        ON team_locations.name = teams.team
)
SELECT
    *,
    '{{ invocation_id }}' AS invocation_id
FROM
    FINAL
