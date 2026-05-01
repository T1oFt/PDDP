-- Initialize PostgreSQL with the model4_university dataset.

DROP TABLE IF EXISTS
    submissions,
    assignments,
    enrollments,
    courses,
    students,
    teachers
CASCADE;

CREATE TABLE teachers (
    teacher_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department TEXT NOT NULL
);

CREATE TABLE courses (
    course_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    teacher_id INTEGER NOT NULL REFERENCES teachers(teacher_id),
    semester TEXT NOT NULL
);

CREATE TABLE students (
    student_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    enrollment_year INTEGER NOT NULL
);

CREATE TABLE enrollments (
    student_id INTEGER NOT NULL REFERENCES students(student_id),
    course_id INTEGER NOT NULL REFERENCES courses(course_id),
    enrolled_dt DATE NOT NULL
);

CREATE TABLE assignments (
    assignment_id INTEGER PRIMARY KEY,
    course_id INTEGER NOT NULL REFERENCES courses(course_id),
    due_dt DATE NOT NULL,
    max_score INTEGER NOT NULL
);

CREATE TABLE submissions (
    submission_id INTEGER PRIMARY KEY,
    assignment_id INTEGER NOT NULL REFERENCES assignments(assignment_id),
    student_id INTEGER NOT NULL REFERENCES students(student_id),
    submitted_ts TIMESTAMP NOT NULL,
    score INTEGER NOT NULL
);

\copy teachers (teacher_id, name, department) FROM '/seed/postgres/teachers.csv' CSV HEADER
\copy courses (course_id, title, teacher_id, semester) FROM '/seed/postgres/courses.csv' CSV HEADER
\copy students (student_id, name, group_name, enrollment_year) FROM '/seed/postgres/students.csv' CSV HEADER
\copy enrollments (student_id, course_id, enrolled_dt) FROM '/seed/postgres/enrollments.csv' CSV HEADER
\copy assignments (assignment_id, course_id, due_dt, max_score) FROM '/seed/postgres/assignments.csv' CSV HEADER
\copy submissions (submission_id, assignment_id, student_id, submitted_ts, score) FROM '/seed/postgres/submissions.csv' CSV HEADER
