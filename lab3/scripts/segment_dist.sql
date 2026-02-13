SELECT gp_segment_id, COUNT(*), ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percent
FROM tdf_finishers_v2
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

SELECT gp_segment_id, COUNT(*), ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percent
FROM tdf_stages_v2
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

SELECT gp_segment_id, COUNT(*), ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percent
FROM tdf_tours_v2
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;

SELECT gp_segment_id, COUNT(*), ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percent
FROM tdf_winners_v2
GROUP BY gp_segment_id 
ORDER BY gp_segment_id;