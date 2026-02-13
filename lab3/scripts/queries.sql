------------------- Сравнение общей статистики турнов с победителями -------------------------
EXPLAIN ANALYZE
SELECT 
    t."Year",
    t."Starters",
    t."Finishers",
    w."Rider" as winner_name,
    w."Country",
    w."Stages_Won"
FROM tdf_tours t
JOIN tdf_winners w ON t."Year" = w."Year"
WHERE t."Year" >= 2000
ORDER BY t."Year" DESC;

EXPLAIN ANALYZE
SELECT 
    t."Year",
    t."Starters",
    t."Finishers",
    w."Rider" as winner_name,
    w."Country",
    w."Stages_Won"
FROM tdf_tours_v2 t
JOIN tdf_winners_v2 w ON t."Year" = w."Year"
WHERE t."Year" >= 2000
ORDER BY t."Year" DESC;

------------------ Найти гонщиков, которые выиграли тур и также выигрывали этапы --------------------
EXPLAIN ANALYZE
SELECT 
    w."Year",
    w."Rider" as tour_winner,
    w."Country",
    COUNT(DISTINCT s."Stage") as stages_won_count,
    STRING_AGG(DISTINCT s."Type", ', ') as stage_types
FROM tdf_winners w
LEFT JOIN tdf_stages s ON w."Rider" = s."Winner"
WHERE w."Year" >= 2010
GROUP BY w."Year", w."Rider", w."Country"
ORDER BY w."Year" DESC, stages_won_count DESC;

EXPLAIN ANALYZE
SELECT 
    w."Year",
    w."Rider" as tour_winner,
    w."Country",
    COUNT(DISTINCT s."Stage") as stages_won_count,
    STRING_AGG(DISTINCT s."Type", ', ') as stage_types
FROM tdf_winners_v2 w
LEFT JOIN tdf_stages_v2 s ON w."Rider" = s."Winner"
WHERE w."Year" >= 2010
GROUP BY w."Year", w."Rider", w."Country"
ORDER BY w."Year" DESC, stages_won_count DESC;

-------------------- Статистика финишировавших с информацией о турнире ------------------------
EXPLAIN ANALYZE
SELECT 
    f."Year",
    f."Rider",
    f."Team",
    f."Rank",
    t."Starters",
    t."Finishers",
    t."Distance",
    ROUND(100.0 * t."Finishers" / NULLIF(t."Starters", 0), 2) as completion_rate
FROM tdf_finishers f
JOIN tdf_tours t ON f."Year" = t."Year"
WHERE f."Year" IN (2015, 2016, 2017)
  AND CAST(f."Rank" AS INTEGER) <= 10
ORDER BY f."Year", CAST(f."Rank" AS INTEGER);

EXPLAIN ANALYZE
SELECT 
    f."Year",
    f."Rider",
    f."Team",
    f."Rank",
    t."Starters",
    t."Finishers",
    t."Distance",
    ROUND(100.0 * t."Finishers" / NULLIF(t."Starters", 0), 2) as completion_rate
FROM tdf_finishers_v2 f
JOIN tdf_tours_v2 t ON f."Year" = t."Year"
WHERE f."Year" IN (2015, 2016, 2017)
  AND CAST(f."Rank" AS INTEGER) <= 10
ORDER BY f."Year", CAST(f."Rank" AS INTEGER);
