-- Databricks notebook source
-- MAGIC %md
-- MAGIC **_PLAN_**
-- MAGIC
-- MAGIC - Create a long table which has all the match data, like runs scored by batsman, wickets by bowler with their stats (DIM)
-- MAGIC - match data table which will have all the match related data like potm, extra runs, venue etc (DIM)
-- MAGIC - fact table to connect above two dim tables (FACT)

-- COMMAND ----------

use catalog ipl_database

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC select * from silver.batsman_stats limit 10

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC select * from silver.match_details limit 5

-- COMMAND ----------

drop table if exists ipl_database.gold.batsman_details

-- COMMAND ----------

create table if not exists ipl_database.gold.batsman_details as 
with add_batsman_stats as (
  select 
  md.match_UUID,
  bs.player_name as batsman_name,
  bs.runs_scored as runs_in_match,
  bs.balls_played as balls_played_in_match,
  bs.boundries as boundries_in_match,
  bs.sixes as sixes_in_match,
  bs.Strike_rate as strike_rate_in_match,
  bs.is_out as gotten_out_in_match,
  bs.is_run_out as gotten_runout_in_match,
  bs.wicket_by_player,
  pd.team as batsman_team,
  case when pd.team == md.team_1 then md.team_2 else md.team_1 end as batsman_opposition_team,
  pd.role as batsman_primary_role
  from silver.batsman_stats bs
  inner join silver.match_details md
  on bs.match_UUID = md.match_UUID
  inner join silver.player_details pd
  on bs.player_name = pd.player_name
),
add_bowler_name(
  select 
  distinct
  bts.wicket_by_player,
  bs.player_name,
  levenshtein(lower(regexp_replace(bts.wicket_by_player, ' ', '')),lower(regexp_replace(bs.player_name, ' ', ''))) as lev,
  levenshtein(lower(substring_index(bs.player_name, ' ', 1)),lower(bts.wicket_by_player)) as lev_first_nm,
  levenshtein(lower(substring_index(bs.player_name, ' ', -1)),lower(bts.wicket_by_player)) as lev_last_nm,
  least(lev,lev_first_nm,lev_last_nm) as min_param
  from add_batsman_stats bts
  inner join silver.player_details bs
  on bts.batsman_opposition_team = bs.team
  where (wicket_by_player <> '-' and lower(regexp_replace(bts.wicket_by_player, ' ', '')) not like 'runout%')
  order by wicket_by_player desc, lev
),
adding_bowler_name as (
  select 
  bs.match_UUID,
  bs.batsman_name,
  bs.runs_in_match,
  bs.balls_played_in_match,
  bs.boundries_in_match,
  bs.sixes_in_match,
  bs.strike_rate_in_match,
  bs.gotten_out_in_match,
  bs.gotten_runout_in_match,
  bs.batsman_team,
  abn.player_name as wicket_taken_by_bowler_name
  from add_batsman_stats bs
  left join add_bowler_name abn
  on bs.wicket_by_player = abn.wicket_by_player
  and abn.min_param = 0
)
-- add_bowler_details(
--   select 
--   bn.*,
--   bs.wickets_in_match,
--   bs.runs_conceded_in_match,
--   bs.non_valid_deliveries,
--   bs.wide_deliveries,
--   bs.sixes_conceded,
--   bs.fours_conceded,
--   bs.dot_deliveries,
--   bs.match_economy,
--   bs.maiden_overs_in_match,
--   bs.overs_bowled_in_match
--   from adding_bowler_name bn
--   left join silver.bowler_stats bs
--   on bn.bowler_name = bs.player_name
--   and bn.match_UUID = bs.match_UUID
-- )
select * from adding_bowler_name order by match_uuid;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 02 Match Details fact table

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC select * from ipl_database.silver.match_details

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC with cte1 as (
-- MAGIC   select 
-- MAGIC   match_UUID,
-- MAGIC   extra_runs,
-- MAGIC   row_number() over(partition by match_UUID order by extra_runs) as rn
-- MAGIC   from ipl_database.silver.extra_runs
-- MAGIC )
-- MAGIC select 
-- MAGIC match_uuid,
-- MAGIC max(case when rn = 1 then extra_runs else null end) as team1_extra_runs,
-- MAGIC max(case when rn = 2 then extra_runs else null end) as team2_extra_runs
-- MAGIC from cte1
-- MAGIC group by 1

-- COMMAND ----------

drop table ipl_database.gold.match_details;

-- COMMAND ----------

create table if not exists ipl_database.gold.match_details as
with cte1 as (
select 
md.*,
case when md.team_1 = er.team then er.extra_runs else null end as team1_extra_runs,
case when md.team_2 = er.team then er.extra_runs else null end as team2_extra_runs,
er.extra_runs
from ipl_database.silver.match_details md 
join ipl_database.silver.extra_runs er
on md.match_UUID = er.match_UUID),
cte2 as 
(select 
match_uuid,
match_number,
match_date,
case when toss_winner = team_1 and toss_decision = 'elected to field first' then team_1 else team_2 end as batting_second,
case when batting_second = team_1 then team_2 else team_1 end as batting_first,
team1_extra_runs as batting_first_extra_runs,
lead(team2_extra_runs) over(order by match_number) as batting_second_extra_runs,
venue,
toss_winner,
toss_decision,
match_winner,
is_day,
season
from cte1
qualify batting_second_extra_runs is not null
)
select  
a.*,
p.player as potm
from cte2 a
join ipl_database.silver.potm p
on a.match_UUID = p.match_UUID
order by a.match_number

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC create table if not exists ipl_database.gold.match_details as
-- MAGIC select match_UUID,
-- MAGIC match_number,
-- MAGIC match_date,
-- MAGIC team_1,
-- MAGIC team_2,
-- MAGIC venue,
-- MAGIC toss_winner,
-- MAGIC toss_decision,
-- MAGIC match_winner,
-- MAGIC is_day,
-- MAGIC season
-- MAGIC from ipl_database.silver.match_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 03 Bowlers gold table

-- COMMAND ----------

drop table ipl_database.gold.bowler_details

-- COMMAND ----------

create table if not exists ipl_database.gold.bowler_details as
select 
bs.*,
pd.team as bowler_team
from ipl_database.silver.bowler_stats bs
inner join ipl_database.silver.match_details md
on bs.match_UUID = md.match_UUID
inner join ipl_database.silver.player_details pd
on bs.player_name = pd.player_name
order by bs.match_UUID

-- COMMAND ----------

select * from ipl_database.gold.match_details

-- COMMAND ----------

select * from ipl_database.silver.potm
where match_UUID in (
select match_UUID from ipl_database.silver.potm
group by 1
having count(*) > 1)

-- COMMAND ----------

select match_UUID,count(*) from ipl_database.silver.extra_runs
group by 1
having count(*) > 1