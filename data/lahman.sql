-- 1. Find all players in the database who played at Vanderbilt University. Create a list showing each player's first and last names as well as the total salary they earned in the major leagues. Sort this list in descending order by the total salary earned. Which Vanderbilt player earned the most money in the majors?
SELECT namefirst || ' ' || namelast, SUM(salary) AS sum_sal
FROM collegeplaying AS c
INNER JOIN people AS p
USING(playerid)
INNER JOIN salaries AS s
ON p.playerid = s.playerid AND s.yearid = c.yearid
WHERE schoolid = 'vandy'
GROUP BY namefirst, namelast
ORDER BY sum_sal DESC;

-- 2. Using the fielding table, group players into three groups based on their position: label players with position OF as "Outfield", those with position "SS", "1B", "2B", and "3B" as "Infield", and those with position "P" or "C" as "Battery". Determine the number of outputs made by each of these three groups in 2016.
SELECT CASE
			WHEN pos = 'OF' THEN 'Outfield'
			WHEN pos = 'SS' THEN 'Infield'
			WHEN pos = '1B' THEN 'Infield'
			WHEN pos = '2B' THEN 'Infield'
			WHEN pos = '3B' THEN 'Infield'
			WHEN pos = 'P' THEN 'Battery'
			WHEN pos = 'C' THEN 'Battery'
			ELSE 'N/A'
		END AS position, COUNT(*) AS outputs
FROM fielding
WHERE yearid = 2016
GROUP BY position;

-- 3. Find the average number of strikeouts per game by decade since 1920. Round the numbers you report to 2 decimal places. Do the same for home runs per game. Do you see any trends? (Hint: For this question, you might find it helpful to look at the **generate_series** function (https://www.postgresql.org/docs/9.1/functions-srf.html). If you want to see an example of this in action, check out this DataCamp video: https://campus.datacamp.com/courses/exploratory-data-analysis-in-sql/summarizing-and-aggregating-numeric-data?ex=6)
WITH years AS (
	SELECT generate_series(1920, 2020, 10) AS decades
	)
SELECT decades, ROUND(SUM(so) * 1.0/SUM(g), 2) AS avg_strikeouts_per_game
FROM teams AS t
INNER JOIN years
ON t.yearid < (decades + 10) AND t.yearid >= decades
GROUP BY decades;

WITH years AS (
	SELECT generate_series(1920, 2020, 10) AS decades
	)
SELECT decades, ROUND(SUM(hr) * 1.0/SUM(g), 2) AS avg_homeruns_per_game
FROM teams AS t
INNER JOIN years
ON t.yearid < (decades + 10) AND t.yearid >= decades
GROUP BY decades;

-- 4. Find the player who had the most success stealing bases in 2016, where __success__ is measured as the percentage of stolen base attempts which are successful. (A stolen base attempt results either in a stolen base or being caught stealing.) Consider only players who attempted _at least_ 20 stolen bases. Report the players' names, number of stolen bases, number of attempts, and stolen base percentage.
SELECT p.namefirst, p.namelast, SUM(b.sb) AS stolen_bases, SUM(b.cs) AS caught_stealing, SUM(b.sb+b.cs) AS stolen_attempts, (100.0*SUM(b.sb)/SUM(b.sb+b.cs)) AS percentage_stolen
FROM people AS p
INNER JOIN batting AS b
USING(playerid)
WHERE b.yearid = 2016
GROUP BY namefirst, namelast
HAVING SUM(b.sb+b.cs) >= 20
ORDER BY percentage_stolen DESC;

-- 5. From 1970 to 2016, what is the largest number of wins for a team that did not win the world series? What is the smallest number of wins for a team that did win the world series? Doing this will probably result in an unusually small number of wins for a world series champion; determine why this is the case. Then redo your query, excluding the problem year. How often from 1970 to 2016 was it the case that a team with the most wins also won the world series? What percentage of the time?
SELECT teamid, yearid, MAX(W) AS max_winnings
FROM teams
WHERE yearid BETWEEN 1970 AND 2016 AND WSWin = 'N'
GROUP BY teamid, yearid
ORDER BY max_winnings DESC; 

SELECT teamid, yearid, MAX(W) AS max_winnings
FROM teams
WHERE yearid BETWEEN 1970 AND 2016 AND WSWin = 'Y' AND yearid <> 1981
GROUP BY teamid, yearid
ORDER BY max_winnings ASC;

SELECT teamid, yearid, MAX(W) AS max_winnings, WSWin
FROM teams
WHERE yearid BETWEEN 1970 AND 2016
GROUP BY teamid, yearid, WSWin
ORDER BY max_winnings DESC;

-- 6. Which managers have won the TSN Manager of the Year award in both the National League (NL) and the American League (AL)? Give their full name and the teams that they were managing when they won the award.
WITH nat_league AS (
	SELECT DISTINCT playerid, lgid
	FROM awardsmanagers
	WHERE awardid = 'TSN Manager of the Year' AND lgid = 'NL'
)
SELECT playerid
FROM awardsmanagers a
INNER JOIN nat_league n
USING(playerid)
WHERE a.awardid = 'TSN Manager of the Year' AND a.lgid = 'AL';


WITH award_managers AS (
	SELECT playerid, a.yearid AS AL_year, NL_year
	FROM awardsmanagers a
	INNER JOIN (
		SELECT DISTINCT playerid, lgid, yearid AS NL_year
		FROM awardsmanagers
		WHERE awardid = 'TSN Manager of the Year' AND lgid = 'NL'
		) AS n
	USING(playerid)
	WHERE a.awardid = 'TSN Manager of the Year' AND a.lgid = 'AL'
	)
SELECT namegiven, AL_year, NL_year, teamid
FROM people p
INNER JOIN award_managers a
USING(playerid)
INNER JOIN managers m
ON m.playerid = p.playerid AND (a.AL_year = m.yearid OR a.NL_year = m.yearid)



-- 7. Which pitcher was the least efficient in 2016 in terms of salary / strikeouts? Only consider pitchers who started at least 10 games (across all teams). Note that pitchers often play for more than one team in a season, so be sure that you are counting all stats for each player.
SELECT namegiven, SUM(s.salary) AS sal, SUM(pi.so) AS strikeouts
FROM pitching AS pi
INNER JOIN people AS p
ON pi.playerid = p.playerid
INNER JOIN salaries AS s
ON pi.playerid = s.playerid AND pi.yearid = s.yearid
WHERE s.yearid = 2016
GROUP BY namegiven
ORDER BY namegiven, sal DESC, strikeouts ASC;



-- 8. Find all players who have had at least 3000 career hits. Report those players' names, total number of hits, and the year they were inducted into the hall of fame (If they were not inducted into the hall of fame, put a null in that column.) Note that a player being inducted into the hall of fame is indicated by a 'Y' in the **inducted** column of the halloffame table.
WITH hall_of_famers AS (
	SELECT playerid, yearid
	FROM halloffame
	WHERE inducted = 'Y'
)
SELECT playerid, namegiven, SUM(H) AS total_hits, h.yearid year_inducted
FROM people p
INNER JOIN batting b
USING(playerid)
LEFT JOIN hall_of_famers h
USING(playerid)
GROUP BY playerid, namegiven, h.yearid
HAVING SUM(H) >= 3000;

-- 9. Find all players who had at least 1,000 hits for two different teams. Report those players' full names.
SELECT *
FROM 

-- 10. Find all players who hit their career highest number of home runs in 2016. Consider only players who have played in the league for at least 10 years, and who hit at least one home run in 2016. Report the players' first and last names and the number of home runs they hit in 2016.
WITH max_homeruns AS(
	SELECT playerid, MAX(hr) AS max_hr
	FROM batting
	GROUP BY playerid
),
hr_2016 AS(
	SELECT playerid, SUM(hr) AS homeruns_2016
	FROM batting
	WHERE yearid = 2016
	GROUP BY playerid
	HAVING SUM(hr) >= 1
),
years_played AS(
	SELECT playerid, COUNT(DISTINCT yearid) AS tot_years_played
	FROM batting
	GROUP BY playerid
	HAVING COUNT(DISTINCT yearid) >= 10
)
SELECT namefirst || ' ' || namelast full_name, mh.max_hr
FROM people p
INNER JOIN max_homeruns mh
USING(playerid)
INNER JOIN years_played yp
USING(playerid)
INNER JOIN hr_2016 h16
USING(playerid)
WHERE mh.max_hr = h16.homeruns_2016;

-- After finishing the above questions, here are some open-ended questions to consider.

-- **Open-ended questions**

-- 11. Is there any correlation between number of wins and team salary? Use data from 2000 and later to answer this question. As you do this analysis, keep in mind that salaries across the whole league tend to increase together, so you may want to look on a year-by-year basis.
SELECT *
FROM salaries;

-- 12. In this question, you will explore the connection between number of wins and attendance.

   -- a. Does there appear to be any correlation between attendance at home games and number of wins?  
   -- b. Do teams that win the world series see a boost in attendance the following year? What about teams that made the playoffs? Making the playoffs means either being a division winner or a wild card winner.


-- 13. It is thought that since left-handed pitchers are more rare, causing batters to face them less often, that they are more effective. Investigate this claim and present evidence to either support or dispute this claim. First, determine just how rare left-handed pitchers are compared with right-handed pitchers. Are left-handed pitchers more likely to win the Cy Young Award? Are they more likely to make it into the hall of fame?
