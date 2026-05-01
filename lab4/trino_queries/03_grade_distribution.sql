WITH course_totals AS (
	SELECT 
	    course_id, 
	    COUNT(DISTINCT student_id) AS total_students
	FROM postgres.public.enrollments 
	GROUP BY 1
),
assignment_stats AS (
    SELECT
        a.assignment_id,
        c.course_id,
        c.title AS course_title,
        a.max_score,
        AVG(s.score) AS mean_score,
        approx_percentile(s.score, 0.5) AS median_score,
        AVG(CASE WHEN s.score = 0 THEN 1.0 ELSE 0.0 END) AS zero_score_share,
        1.0 - (COUNT(DISTINCT s.student_id) * 1.0 / NULLIF(ct.total_students, 0)) AS missing_submission_share,
        AVG(CASE WHEN date(s.submitted_ts) > a.due_dt THEN 1.0 ELSE 0.0 END) AS delay_share,
        AVG(CAST(s.score AS INTEGER)) / NULLIF(a.max_score, 0) AS mean_score_ratio
    FROM postgres.public.assignments a
    JOIN postgres.public.courses c
        ON c.course_id = a.course_id
    JOIN course_totals ct 
    	ON ct.course_id = c.course_id
    LEFT JOIN postgres.public.submissions s
        ON s.assignment_id = a.assignment_id
    GROUP BY 1, 2, 3, 4, ct.total_students
),
thresholds AS (
    SELECT
        AVG(mean_score_ratio) AS mean_score_ratio_threshold,
        AVG(delay_share) AS delay_share_threshold
    FROM assignment_stats
)
SELECT
    a.assignment_id,
    a.course_id,
    a.course_title,
    a.max_score,
    a.mean_score,
    a.median_score,
    a.zero_score_share,
    a.missing_submission_share,
    a.delay_share,
    a.mean_score_ratio,
    t.mean_score_ratio_threshold,
    t.delay_share_threshold,
    (a.mean_score_ratio < t.mean_score_ratio_threshold AND a.delay_share > t.delay_share_threshold) AS too_hard
FROM assignment_stats a
CROSS JOIN thresholds t
ORDER BY a.mean_score_ratio ASC, a.delay_share DESC, a.assignment_id;