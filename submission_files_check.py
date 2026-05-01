import pandas as pd


subs = pd.read_csv('lab4/model4_university/postgres/submissions.csv')

files = pd.read_csv('lab4/model4_university/minio/submission_files_manifest.csv')

print(subs.head())
print(files.head())

print(set(subs['submission_id']) - set(files['submission_id']))