SELECT 
    COUNT(DISTINCT "Table_name") AS distinct_table_name,
    COUNT(DISTINCT "Field_name") AS distinct_field_name,
    COUNT(DISTINCT "Description") AS distinct_description
FROM ext_data_dictionary;

SELECT 
    COUNT(DISTINCT "Year") AS distinct_year,
    COUNT(DISTINCT "Rank") AS distinct_rank,
    COUNT(DISTINCT "Rider") AS distinct_rider,
    COUNT(DISTINCT "Time") AS distinct_time,
    COUNT(DISTINCT "Team") AS distinct_team
FROM ext_tdf_finishers;

SELECT 
    COUNT(DISTINCT "Year") AS distinct_year,
    COUNT(DISTINCT "Date") AS distinct_date,
    COUNT(DISTINCT "Stage") AS distinct_stage,
    COUNT(DISTINCT "Course") AS distinct_course,
    COUNT(DISTINCT "Distance") AS distinct_distance,
    COUNT(DISTINCT "Type") AS distinct_type,
    COUNT(DISTINCT "Winner") AS distinct_winner
FROM ext_tdf_stages;

SELECT 
    COUNT(DISTINCT "Year") AS distinct_year,
    COUNT(DISTINCT "Dates") AS distinct_dates,
    COUNT(DISTINCT "Stages") AS distinct_stages,
    COUNT(DISTINCT "Distance") AS distinct_distance,
    COUNT(DISTINCT "Starters") AS distinct_starters,
    COUNT(DISTINCT "Finishers") AS distinct_finishers
FROM ext_tdf_tours;

SELECT 
    COUNT(DISTINCT "Year") AS distinct_year,
    COUNT(DISTINCT "Country") AS distinct_country,
    COUNT(DISTINCT "Rider") AS distinct_rider,
    COUNT(DISTINCT "Team") AS distinct_team,
    COUNT(DISTINCT "Time") AS distinct_time,
    COUNT(DISTINCT "Margin") AS distinct_margin,
    COUNT(DISTINCT "Stages_Won") AS distinct_stages_won,
    COUNT(DISTINCT "Stages_Led") AS distinct_stages_led,
    COUNT(DISTINCT "Avg_Speed") AS distinct_avg_speed,
    COUNT(DISTINCT "Height") AS distinct_height,
    COUNT(DISTINCT "Weight") AS distinct_weight,
    COUNT(DISTINCT "Born") AS distinct_born,
    COUNT(DISTINCT "Died") AS distinct_died
FROM ext_tdf_winners;