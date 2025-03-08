-- Question 1: Rankings
-- Question 1a: Warmup Question
-- Write a query which retrieves each teamid and number of wins (w) for the 2016 season. Apply three window functions to the number of wins (ordered in descending order) - ROW_NUMBER, RANK, AND DENSE_RANK. Compare the output from these three functions. What do you notice?
SELECT 
	teamid,
	w,
	ROW_NUMBER() OVER(ORDER BY w DESC),
	RANK() OVER(ORDER BY w DESC),
	DENSE_RANK() OVER(ORDER BY w DESC)
FROM teams
WHERE yearid = 2016;

-- Question 1b: 
-- Which team has finished in last place in its division (i.e. with the least number of wins) the most number of times? A team's division is indicated by the divid column in the teams table.
WITH window_cte AS(	
	SELECT 
		teamid, 
		divid,
		yearid,
		RANK() OVER(PARTITION BY lgid, yearid, divid ORDER BY w ASC) AS rank_total_wins
	FROM teams
	)
SELECT teamid, COUNT(rank_total_wins) AS count_ranks
FROM window_cte
WHERE rank_total_wins = 1
GROUP BY teamid
ORDER BY count_ranks DESC;

-- Question 2: Cumulative Sums
-- Question 2a: 
-- Barry Bonds has the record for the highest career home runs, with 762. Write a query which returns, for each season of Bonds' career the total number of seasons he had played and his total career home runs at the end of that season. (Barry Bonds' playerid is bondsba01.)
SELECT
	yearid,
	RANK() OVER(ORDER BY yearid),
	SUM(hr) OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM batting
WHERE playerid = 'bondsba01';

-- Question 2b:
-- How many players at the end of the 2016 season were on pace to beat Barry Bonds' record? For this question, we will consider a player to be on pace to beat Bonds' record if they have more home runs than Barry Bonds had the same number of seasons into his career. 
WITH barry_bonds_hr AS(
	SELECT
		yearid,
		DENSE_RANK() OVER(ORDER BY yearid) AS season_rank,
		SUM(hr) OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS barry_hr_rolling_total
	FROM batting
	WHERE playerid = 'bondsba01'
),
player_hr AS(
	SELECT 
		playerid,
		yearid,
		DENSE_RANK() OVER(PARTITION BY playerid ORDER BY yearid) AS season_rank,
		SUM(hr) OVER(PARTITION BY playerid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_total_hr
	FROM batting
	WHERE yearid <= 2016
)
SELECT 
	ph.playerid
FROM player_hr ph
JOIN barry_bonds_hr bbh
ON ph.season_rank = bbh.season_rank AND ph.yearid = 2016 AND ph.rolling_total_hr > bbh.barry_hr_rolling_total;

-- Question 2c: 
-- Were there any players who 20 years into their career who had hit more home runs at that point into their career than Barry Bonds had hit 20 years into his career? 
WITH barry_bonds_hr AS(
	SELECT
		yearid,
		DENSE_RANK() OVER(ORDER BY yearid) AS season_rank,
		SUM(hr) OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS barry_hr_rolling_total
	FROM batting
	WHERE playerid = 'bondsba01'
),
player_hr AS(
	SELECT 
		playerid,
		yearid,
		DENSE_RANK() OVER(PARTITION BY playerid ORDER BY yearid) AS season_rank,
		SUM(hr) OVER(PARTITION BY playerid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_total_hr
	FROM batting
)
SELECT 
	ph.playerid,
	ph.rolling_total_hr,
	bbh.barry_hr_rolling_total
FROM player_hr ph
INNER JOIN barry_bonds_hr bbh
USING(season_rank)
WHERE ph.season_rank = 20 AND ph.rolling_total_hr > bbh.barry_hr_rolling_total;

-- Question 3: Anomalous Seasons
-- Find the player who had the most anomalous season in terms of number of home runs hit. To do this, find the player who has the largest gap between the number of home runs hit in a season and the 5-year moving average number of home runs if we consider the 5-year window centered at that year (the window should include that year, the two years prior and the two years after).
WITH player_window AS(
	SELECT 
		playerid,
		yearid,
		hr,
		ROUND(AVG(hr) OVER(PARTITION BY playerid ORDER BY yearid ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING), 2) AS avg_year_window
	FROM batting
)
SELECT pw.*, hr - pw.avg_year_window AS largest_difference
FROM player_window AS pw
ORDER BY largest_difference DESC;

-- Question 4: Players Playing for one Team
-- this question, we'll just consider players that appear in the batting table.
-- Question 4a: 
-- Warmup: How many players played at least 10 years in the league and played for exactly one team? (For this question, exclude any players who played in the 2016 season). Who had the longest career with a single team? (You can probably answer this question without needing to use a window function.)
WITH season_rankings AS(
	SELECT 
		playerid,
		RANK() OVER(PARTITION BY playerid ORDER BY yearid) AS seasons_played
	FROM batting
	WHERE yearid != 2016
)
SELECT 
	DISTINCT ON (teamid)
	playerid,
	COUNT(teamid) AS number_team_played_for
FROM season_rankings sr
JOIN batting
USING(playerid)
WHERE seasons_played = 10
GROUP BY playerid, teamid

-- Question 4b: 
-- Some players start and end their careers with the same team but play for other teams in between. For example, Barry Zito started his career with the Oakland Athletics, moved to the San Francisco Giants for 7 seasons before returning to the Oakland Athletics for his final season. How many players played at least 10 years in the league and start and end their careers with the same team but played for at least one other team during their career? For this question, exclude any players who played in the 2016 season.
WITH season_rankings AS(
	SELECT 
		playerid,
		RANK() OVER(PARTITION BY playerid ORDER BY yearid) AS seasons_played
	FROM batting
	WHERE yearid != 2016
)
SELECT *
FROM batting

-- Question 5: Streaks
-- Question 5a: 
-- How many times did a team win the World Series in consecutive years?
WITH year_rank AS(
	SELECT 
		teamid,
		yearid,
		LAG(yearid, 1) OVER(PARTITION BY teamid ORDER BY yearid)
	FROM teams
	WHERE wswin = 'Y'
)
SELECT 
	teamid,
	COUNT(lag) OVER(PARTITION BY teamid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM year_rank
WHERE lag = yearid -1;

-- Question 5b: 
-- What is the longest steak of a team winning the World Series? Write a query that produces this result rather than scanning the output of your previous answer.
WITH cons_wswin AS(
	SELECT 
		teamid,
		COUNT(lag) OVER(PARTITION BY teamid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
	FROM (SELECT 
			teamid,
			yearid,
			LAG(yearid, 1) OVER(PARTITION BY teamid ORDER BY yearid)
		FROM teams
		WHERE wswin = 'Y')
	WHERE lag = yearid -1
)
SELECT
	teamid,
	MAX(count)
FROM cons_wswin
GROUP BY teamid


-- Question 5c: 
-- A team made the playoffs in a year if either divwin, wcwin, or lgwin will are equal to 'Y'. Which team has the longest streak of making the playoffs? 


-- Question 5d: 
-- The 1994 season was shortened due to a strike. If we don't count a streak as being broken by this season, does this change your answer for the previous part?

-- Question 6: Manager Effectiveness
-- Which manager had the most positive effect on a team's winning percentage?
-- To determine this, calculate the average winning percentage in the three years before the manager's first full season and compare it to the average winning percentage for that manager's 2nd through 4th full season. 
-- Consider only managers who managed at least 4 full years at the new team and teams that had been in existence for at least 3 years prior to the manager's first full season.
SELECT *
FROM managers m
JOIN teams t
ON m.teamid = t.teamid AND m.yearid = t.yearid
WHERE inseason = 0

SELECT *
FROM managers m