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
final AS (
    SELECT
        teams.team,
        team_locations.city,
        team_locations.state
    FROM
        teams
        LEFT JOIN team_locations
        ON team_locations.name = teams.team
)
SELECT
    *
FROM
    final
