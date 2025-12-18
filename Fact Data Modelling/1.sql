WITH deduped AS (
    SELECT *, row_number() over (
        PARTITION BY game_id,team_id, player_id
        ) as row_num
        FROM game_details
)

SELECT *
FROM deduped
WHERE row_num = 1

