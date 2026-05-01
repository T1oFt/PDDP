SELECT 
    c.course_id, 
    c.title,
    COUNT(DISTINCT e.student_id) AS students_count,
    COUNT(distinct a.assignment_id) AS assignments_count
FROM 
    postgres.public.courses c
LEFT JOIN 
    postgres.public.enrollments e 
ON 
    c.course_id = e.course_id
LEFT JOIN 
    postgres.public.assignments a 
ON 
    c.course_id = a.course_id
GROUP BY 
    c.course_id, c.title
ORDER BY 
    students_count desc, assignments_count desc, course_id
LIMIT 5;