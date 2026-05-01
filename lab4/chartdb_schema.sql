-- Schema for ChartDB import.
-- This file models the data sources used in Trino:
-- PostgreSQL tables, MongoDB collection, and the MinIO manifest.

CREATE TABLE teachers (
    teacher_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department TEXT NOT NULL
);

CREATE TABLE courses (
    course_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    teacher_id INTEGER NOT NULL,
    semester TEXT NOT NULL,
    CONSTRAINT fk_courses_teacher
        FOREIGN KEY (teacher_id) REFERENCES teachers (teacher_id)
);

CREATE TABLE students (
    student_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    enrollment_year INTEGER NOT NULL
);

CREATE TABLE enrollments (
    student_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    enrolled_dt DATE NOT NULL,
    CONSTRAINT fk_enrollments_student
        FOREIGN KEY (student_id) REFERENCES students (student_id),
    CONSTRAINT fk_enrollments_course
        FOREIGN KEY (course_id) REFERENCES courses (course_id)
);

CREATE TABLE assignments (
    assignment_id INTEGER PRIMARY KEY,
    course_id INTEGER NOT NULL,
    due_dt DATE NOT NULL,
    max_score INTEGER NOT NULL,
    CONSTRAINT fk_assignments_course
        FOREIGN KEY (course_id) REFERENCES courses (course_id)
);

CREATE TABLE submissions (
    submission_id INTEGER PRIMARY KEY,
    assignment_id INTEGER NOT NULL,
    student_id INTEGER NOT NULL,
    submitted_ts TIMESTAMP NOT NULL,
    score INTEGER NOT NULL,
    CONSTRAINT fk_submissions_assignment
        FOREIGN KEY (assignment_id) REFERENCES assignments (assignment_id),
    CONSTRAINT fk_submissions_student
        FOREIGN KEY (student_id) REFERENCES students (student_id)
);

-- MongoDB collection represented as a table for ERD visualization.
CREATE TABLE submission_feedback (
    _id TEXT PRIMARY KEY,
    submission_id INTEGER NOT NULL,
    teacher_id INTEGER NOT NULL,
    ts TIMESTAMP NOT NULL,
    rubric TEXT, -- serialized rubric array/document
    overall_comment TEXT,
    CONSTRAINT fk_feedback_submission
        FOREIGN KEY (submission_id) REFERENCES submissions (submission_id),
    CONSTRAINT fk_feedback_teacher
        FOREIGN KEY (teacher_id) REFERENCES teachers (teacher_id)
);

-- MinIO object manifest used by Trino.
CREATE TABLE submission_files_manifest (
    assignment_id VARCHAR,
    student_id VARCHAR,
    submission_id VARCHAR,
    file_path VARCHAR,
    size_kb VARCHAR,
    CONSTRAINT fk_manifest_submission
        FOREIGN KEY (submission_id) REFERENCES submissions (submission_id),
    CONSTRAINT fk_manifest_assignment
        FOREIGN KEY (assignment_id) REFERENCES assignments (assignment_id),
    CONSTRAINT fk_manifest_student
        FOREIGN KEY (student_id) REFERENCES students (student_id)
);
