WITH correctness_by_submission AS (
    SELECT
        sf.submission_id,
        AVG(CAST(points AS INTEGER)) AS correctness_points
    FROM mongodb.analytics_db.submission_feedback sf
    CROSS JOIN UNNEST(sf.rubric) AS t(criterion, points, comment)
    WHERE criterion = 'correctness'
    GROUP BY 1
),
submission_facts AS (
    SELECT
        c.course_id,
        c.title AS course_title,
        s.submission_id,
        s.score,
        a.due_dt,
        s.submitted_ts,
        cast(m.size_kb as INTEGER) as "size_kb",
        cb.correctness_points
    FROM postgres.public.courses c
    left JOIN postgres.public.assignments a
        ON a.course_id = c.course_id
    left JOIN postgres.public.submissions s
        ON s.assignment_id = a.assignment_id
    LEFT JOIN minio.default.submission_files_manifest m
        ON cast(m.submission_id as INTEGER) = s.submission_id
    LEFT JOIN correctness_by_submission cb
        ON cb.submission_id = s.submission_id
)
SELECT
    course_id,
    course_title,
    AVG(score)                                          AS avg_score,
    AVG(CASE WHEN date(submitted_ts) > due_dt THEN 1.0 ELSE 0.0 END)  AS delay_share,
    AVG(size_kb)                                        AS avg_file_size_kb,
    AVG(correctness_points)                                             AS avg_correctness_points
FROM submission_facts
GROUP BY 1, 2
ORDER BY course_id;