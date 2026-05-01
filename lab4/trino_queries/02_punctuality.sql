WITH delays AS (
    SELECT
        c.course_id,
        st.group_name,
        s.submission_id,
        date_diff('day', a.due_dt, date(s.submitted_ts)) AS delay_days
    FROM postgres.public.submissions s
    JOIN postgres.public.assignments a
        ON a.assignment_id = s.assignment_id
    JOIN postgres.public.courses c
        ON c.course_id = a.course_id
    JOIN postgres.public.students st
        ON st.student_id = s.student_id
)
SELECT
    'course' AS dimension,
    CAST(course_id AS VARCHAR) AS bucket,
    COUNT(*) AS submissions_count,
    AVG(CASE WHEN delay_days > 0 THEN 1.000 ELSE 0.0 END) AS delay_share,
    AVG(delay_days) AS avg_submission_offset,
    AVG(CASE WHEN delay_days > 0 THEN delay_days END) as avg_delay_days
FROM delays
GROUP BY 1, 2
UNION ALL
SELECT
    'group' AS dimension,
    group_name AS bucket,
    COUNT(*) AS submissions_count,
    AVG(CASE WHEN delay_days > 0 THEN 1.000 ELSE 0.0 END) AS delay_share,
    AVG(delay_days) AS avg_submission_offset,
    AVG(CASE WHEN delay_days > 0 THEN delay_days END) as avg_delay_days
FROM delays
GROUP BY 1, 2
ORDER BY dimension, delay_share DESC, bucket;