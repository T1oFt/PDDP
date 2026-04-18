import csv

input_file = 'lab2/producer/data/instagram_usage_lifestyle.csv'
output_file = 'lab2/producer/data/instagram_usage_lifestyle_10000.csv'

with open(input_file, 'r', newline='') as infile, open(output_file, 'w', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    header = next(reader)
    writer.writerow(header)

    user_id_idx = header.index('user_id')

    for i, row in enumerate(reader, 1):
        if i > 10000:
            break

        if i % 10 == 0:
            row[user_id_idx] = ''

        writer.writerow(row)
