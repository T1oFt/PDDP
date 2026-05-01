WITH manifest AS (
    SELECT
        cast(assignment_id as INTEGER) as "assignment_id",
        cast(student_id as INTEGER) as "student_id",
        cast(submission_id as INTEGER) as "submission_id",
        file_path,
        CAST(size_kb AS INTEGER) AS "size_kb"
    FROM minio.default.submission_files_manifest
),
top_files AS (
    SELECT
        assignment_id,
        submission_id,
        size_kb,
        row_number() OVER (
            PARTITION BY assignment_id
            ORDER BY size_kb DESC, submission_id
        ) AS rn
    FROM manifest
),
size_stats AS (
    SELECT
        assignment_id,
        AVG(CAST(size_kb AS INTEGER)) AS avg_file_size_kb
    FROM manifest
    GROUP BY 1
),
top_10_stats AS (
    SELECT
        assignment_id,
        COALESCE(
            CAST(
                ARRAY_AGG(
                    CAST(ROW(submission_id, size_kb) AS ROW(submission_id INTEGER, size_kb INTEGER))
                    ORDER BY size_kb DESC, submission_id
                ) FILTER (WHERE rn <= 10) AS JSON
            ),
            JSON '[]'
        ) AS top_10
    FROM top_files
    GROUP BY 1
),
missing_files AS (
    SELECT
        a.assignment_id,
        COALESCE(
            CAST(
                ARRAY_AGG(s.submission_id ORDER BY s.submission_id)
                    FILTER (WHERE m.submission_id IS NULL) AS JSON
            ),
            JSON '[]'
        ) AS file_missing
    FROM postgres.public.assignments a
    LEFT JOIN postgres.public.submissions s
        ON s.assignment_id = a.assignment_id
    LEFT JOIN manifest m
        ON m.submission_id = s.submission_id
    GROUP BY a.assignment_id
)
SELECT
    a.assignment_id,
    ss.avg_file_size_kb,
    tf.top_10,
    mf.file_missing
FROM postgres.public.assignments a
LEFT JOIN size_stats ss
    ON ss.assignment_id = a.assignment_id
LEFT JOIN top_10_stats tf
    ON tf.assignment_id = a.assignment_id
LEFT JOIN missing_files mf
    ON mf.assignment_id = a.assignment_id
ORDER BY a.assignment_id;