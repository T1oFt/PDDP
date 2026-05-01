-- 4. Оценивание по рубрике
-- Query 1: rubric criteria analytics
-- Query 2: teacher analytics
-- Query 3: course analytics

WITH rubric_items AS (
    SELECT
        sf.submission_id,
        sf.teacher_id,
        criterion as "criterion",
        CAST(points AS INTEGER) as "points",
        comment as "comment",
        sf.overall_comment
    FROM mongodb.analytics_db.submission_feedback sf
    CROSS JOIN UNNEST(sf.rubric) AS t(criterion, points, comment)
),
criterion_stats AS (
    SELECT
        criterion,
        AVG(points) AS avg_points,
        COUNT(*) AS rubric_mentions,
        AVG(CASE WHEN points <= 1 THEN 1.000 ELSE 0.0 END) AS low_points_share,
        COUNT_IF(points <= 1) AS low_points_count
    FROM rubric_items
    GROUP BY 1
)
select * from criterion_stats order by criterion_stats.low_points_share DESC

--------------------------------------------------------------------------------

WITH feedback_base AS (
    SELECT
        sf._id AS feedback_id,
        sf.submission_id,
        sf.teacher_id,
        sf.overall_comment,
        s.score,
        t.name AS teacher_name,
        t.department
    FROM mongodb.analytics_db.submission_feedback sf
    JOIN postgres.public.submissions s
        ON s.submission_id = sf.submission_id
    JOIN postgres.public.teachers t
        ON t.teacher_id = sf.teacher_id
),
rubric_items AS (
    SELECT
        sf._id AS feedback_id,
        criterion,
        CAST(points AS INTEGER) AS "points"
    FROM mongodb.analytics_db.submission_feedback sf
    CROSS JOIN UNNEST(sf.rubric) AS t(criterion, points, comment)
),
feedback_rubric_summary AS (
    SELECT
        feedback_id,
        AVG(points) AS avg_points_per_criterion,
        SUM(points) AS total_points_per_feedback
    FROM rubric_items
    GROUP BY 1
),
comment_counts AS (
    SELECT
        teacher_id,
        overall_comment,
        COUNT(*) AS comment_count
    FROM feedback_base
    GROUP BY 1, 2
)
SELECT
    fb.teacher_id,
    fb.teacher_name,
    fb.department,
    COUNT(*) AS feedback_count,
    COUNT(DISTINCT fb.submission_id) AS reviewed_submissions,
    ROUND(AVG(fb.score), 2) AS avg_submission_score,
    ROUND(AVG(frs.avg_points_per_criterion), 2) AS avg_points_per_criterion,
    ROUND(AVG(frs.total_points_per_feedback), 2) AS avg_total_points_per_feedback,
    (
        SELECT cc.overall_comment
        FROM comment_counts cc
        WHERE cc.teacher_id = fb.teacher_id
        ORDER BY cc.comment_count DESC, cc.overall_comment
        LIMIT 1
    ) AS most_popular_overall_comment
FROM feedback_base fb
JOIN feedback_rubric_summary frs
    ON frs.feedback_id = fb.feedback_id
GROUP BY 1, 2, 3
ORDER BY feedback_count DESC, teacher_id;

--------------------------------------------------------------------------------

WITH feedback_base AS (
    SELECT
        sf._id AS feedback_id,
        sf.submission_id,
        sf.overall_comment,
        s.score,
        c.course_id,
        c.title AS course_title,
        c.semester,
        t.name AS teacher_name
    FROM mongodb.analytics_db.submission_feedback sf
    JOIN postgres.public.submissions s
        ON s.submission_id = sf.submission_id
    JOIN postgres.public.assignments a
        ON a.assignment_id = s.assignment_id
    JOIN postgres.public.courses c
        ON c.course_id = a.course_id
    JOIN postgres.public.teachers t
        ON t.teacher_id = c.teacher_id
),
rubric_items AS (
    SELECT
        sf._id AS feedback_id,
        criterion,
        CAST(points AS INTEGER) AS "points"
    FROM mongodb.analytics_db.submission_feedback sf
    CROSS JOIN UNNEST(sf.rubric) AS t(criterion, points, comment)
),
feedback_rubric_summary AS (
    SELECT
        feedback_id,
        AVG(points) AS avg_points_per_criterion,
        SUM(points) AS total_points_per_feedback,
        MAX(CASE WHEN criterion = 'correctness' THEN points END) AS correctness_points
    FROM rubric_items
    GROUP BY 1
),
comment_counts AS (
    SELECT
        course_id,
        overall_comment,
        COUNT(*) AS comment_count
    FROM feedback_base
    GROUP BY 1, 2
)
SELECT
    fb.course_id,
    fb.course_title,
    fb.semester,
    fb.teacher_name,
    COUNT(*) AS feedback_count,
    COUNT(DISTINCT fb.submission_id) AS reviewed_submissions,
    ROUND(AVG(fb.score), 2) AS avg_submission_score,
    ROUND(AVG(frs.total_points_per_feedback), 2) AS avg_total_points_per_feedback,
    ROUND(AVG(frs.correctness_points), 2) AS avg_correctness_points,
    (
        SELECT cc.overall_comment
        FROM comment_counts cc
        WHERE cc.course_id = fb.course_id
        ORDER BY cc.comment_count DESC, cc.overall_comment
        LIMIT 1
    ) AS most_popular_overall_comment
FROM feedback_base fb
JOIN feedback_rubric_summary frs
    ON frs.feedback_id = fb.feedback_id
GROUP BY 1, 2, 3, 4
ORDER BY feedback_count DESC, course_id;
