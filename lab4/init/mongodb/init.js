// Initialize MongoDB with the model4_university dataset.
// The CSV is mounted read-only into the container at /seed/mongodb.

db = db.getSiblingDB('analytics_db');

const fs = require('fs');
const csvPath = '/seed/mongodb/submission_feedback.csv';

function parseCsv(text) {
    const rows = [];
    let row = [];
    let field = '';
    let inQuotes = false;

    for (let i = 0; i < text.length; i += 1) {
        const ch = text[i];

        if (inQuotes) {
            if (ch === '"') {
                if (text[i + 1] === '"') {
                    field += '"';
                    i += 1;
                } else {
                    inQuotes = false;
                }
            } else {
                field += ch;
            }
            continue;
        }

        if (ch === '"') {
            inQuotes = true;
        } else if (ch === ',') {
            row.push(field);
            field = '';
        } else if (ch === '\n') {
            row.push(field);
            rows.push(row);
            row = [];
            field = '';
        } else if (ch !== '\r') {
            field += ch;
        }
    }

    if (field.length > 0 || row.length > 0) {
        row.push(field);
        rows.push(row);
    }

    return rows;
}

const csvText = fs.readFileSync(csvPath, 'utf8');
const rows = parseCsv(csvText).filter((row) => row.length > 1);
const headers = rows.shift();

const docs = rows.map((row) => {
    const values = {};

    for (let i = 0; i < headers.length; i += 1) {
        values[headers[i]] = row[i];
    }

    return {
        _id: values._id,
        submission_id: Number(values.submission_id),
        teacher_id: Number(values.teacher_id),
        ts: new Date(values.ts.replace(' ', 'T')),
        rubric: JSON.parse(values.rubric),
        overall_comment: values.overall_comment,
    };
});

db.submission_feedback.drop();
db.createCollection('submission_feedback');

for (let i = 0; i < docs.length; i += 100) {
    db.submission_feedback.insertMany(docs.slice(i, i + 100), { ordered: false });
}

db.submission_feedback.createIndex({ submission_id: 1 });
db.submission_feedback.createIndex({ teacher_id: 1 });
