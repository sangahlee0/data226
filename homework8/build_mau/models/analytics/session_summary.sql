/*WITH u AS (
    SELECT * FROM {{ ref("user_session_channel") }}
), st AS (
    SELECT * FROM {{ ref("session_timestamp") }}
)
SELECT u.userId, u.sessionId, u.channel, st.ts
FROM u
JOIN st ON u.sessionId = st.sessionId
*/

SELECT u.*, s.ts
FROM {{ ref("user_session_channel") }} u
JOIN {{ ref("session_timestamp") }} s ON u.sessionId = s.sessionId