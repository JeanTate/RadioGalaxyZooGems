# this script was written in Python 3.6.2 "out of the box" and should run without any added packages.
import csv
import json
import os
import operator
import sys

csv.field_size_limit(sys.maxsize)

# File location section:
# Give full path and filenames for input and output files (these are user specific -
#  The easiest way to get the full path and file name is to copy and paste
# from "Properties" (right click of file name in file explorer.
# Example: location = r'C:\py\AAClass\amazon-aerobotany-classifications_2017-03-18.csv'
location = r'C:\py\ZooGems\radio-galaxy-zoogems-classifications18Feb.csv'
target_location = location.partition('radio')[0] + 'RGZGtargets.csv'
out_location = location.partition('-class')[0] + '_flattened.csv'
sorted_location = location.partition('-class')[0] + '_sorted_log.csv'
aggregate_location = location.partition('-class')[0] + '_raw_log.csv'


def include(class_record):
    #  define a function that returns True or False based on whether the argument record is to be included or not in
    #  the output file based on the conditional clauses.

    if class_record['user_name'] == 'JeanTate':
        return False
    if '2018-02-18 11:00:01 UTC' >= class_record['created_at'] >= '2018-02-02 22:00:00 UTC':
        pass  # replace earliest and latest created_at date and times to select records commenced in a
        #  specific time period
    else:
        return False
    if class_record['user_name'].find('not-logged-in') < 0:
        pass
    else:
        return False
    return True


# Open RGZGtargets.csv and build a dictionary of ID's and positions
with open(target_location) as target_file:
    targetdict = csv.DictReader(target_file)
    target = {}
    for row in targetdict:
        target[row['SubjectID']] = {'Set': row['Set'],
                                    'Jname': row['Jname'],
                                    'RA': row['RA'],
                                    'Dec': row['Dec'],
                                    'SDSS_ObjId': row['SDSS_ObjId']}

# prepare the output file and write the header
with open(out_location, 'w', newline='') as file:
    fieldnames = ['subject_ids',
                  'user_name',
                  'user_ip',
                  'workflow_id',
                  'workflow_version',
                  'created_at',
                  'answer',
                  'value']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    # this area for initializing counters, status lists and loading pick lists into memory:
    i = 0
    j = 0

    #  open the zooniverse data file using dictreader,  and load the more complex json strings as python objects
    with open(location) as f:
        r = csv.DictReader(f)
        for row in r:
            i += 1
            if include(row) is True:
                j += 1
                annotations = json.loads(row['annotations'])
                # Flatten First task and assign values
                first_task = annotations[0]
                task_answer = str(first_task['value'])
                if task_answer.lower().find('lowest') == 0:
                    value = 1
                elif task_answer.lower().find('low') == 0:
                    value = 2
                elif task_answer.lower().find('medium') == 0:
                    value = 3
                elif task_answer.lower().find('highest') == 0:
                    value = 5
                elif task_answer.lower().find('high') == 0:
                    value = 4
                else:
                    value = 0

                # fix duplicate image issue
                subject_ids = row['subject_ids']
                if subject_ids == '18237348':
                    subject_ids = '18834508'
                if subject_ids == '18239960':
                    subject_ids = '18934724'

                # This set up the writer to match the field names above and the variable names of their values:
                writer.writerow({'subject_ids': subject_ids,
                                 'user_name': row['user_name'],
                                 'user_ip': row['user_ip'],
                                 'workflow_id': row['workflow_id'],
                                 'workflow_version': row['workflow_version'],
                                 'created_at': row['created_at'],
                                 'answer': task_answer,
                                 'value': value
                                 })

                print(j)  # just so we know progress is being made
# This area prints some basic process info and status
print(i, 'lines read and inspected', j, 'records processed and copied')


def sort_file(input_file, output_file_sorted, field, reverse, clean):
    #  This allows a sort of the output file on a specific field.
    with open(input_file, 'r') as in_file:
        in_put = csv.reader(in_file, dialect='excel')
        headers = in_put.__next__()
        sort = sorted(in_put, key=operator.itemgetter(field), reverse=reverse)

        with open(output_file_sorted, 'w', newline='') as out_file:
            write_sorted = csv.writer(out_file, delimiter=',')
            write_sorted.writerow(headers)
            sort_counter = 0
            for line in sort:
                write_sorted.writerow(line)
                sort_counter += 1
    if clean:  # clean up temporary file
        try:
            os.remove(input_file)
        except OSError:
            print('temp file not found and deleted')
    return sort_counter


print(sort_file(out_location, sorted_location, 0, False, True), 'lines sorted and written')


# ____________________________________________________________________________________________________________
# This next section aggregates the responses for each subject-species and outputs the result


def process_aggregation(subj, vote_count, workflow, version, value_tot):
    if vote_count > 0:
        try:
            new_row = {'Set': target[subj]['Set'],
                       'Jname': target[subj]['Jname'],
                       'RA': target[subj]['RA'],
                       'Dec': target[subj]['Dec'],
                       'SDSS_ObjId': json.dumps(target[subj]['SDSS_ObjId']),
                       'subject_ids': subj,
                       'voters': vote_count,
                       'workflow_id': workflow,
                       'workflow_version': version,
                       'total_votes': value_tot}
        except KeyError:
            not_found.append(subj)
            new_row = {'Set': '',
                       'Jname': '',
                       'RA': '',
                       'Dec': '',
                       'SDSS_ObjId': '',
                       'subject_ids': subj,
                       'voters': vote_count,
                       'workflow_id': workflow,
                       'workflow_version': version,
                       'total_votes': value_tot}
        writer.writerow(new_row)
        return 1
    else:
        return 0


# set up the output file names for the aggregated and processed data, and write the header.
with open(aggregate_location, 'w', newline='') as file:
    fieldnames = ['subject_ids',
                  'voters',
                  'workflow_id',
                  'workflow_version',
                  'Set',
                  'Jname',
                  'RA',
                  'Dec',
                  'SDSS_ObjId',
                  'total_votes'
                  ]
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    # set up to read the flattened file
    with open(sorted_location) as f:
        r = csv.DictReader(f)

        # initialize a starting point subject and empty bins for aggregation
        subject = ''
        workflow_id = ''
        workflow_version = ''
        user_name = set()
        vote_counter = 0
        value_total = 0
        row_counter = 0
        subjects = 0
        not_found = []

        # Loop over the flattened classification records
        for row1 in r:
            row_counter += 1
            # read a row and pullout the flattened data fields we need to aggregate, or pass through.
            if int(row1['value']) < 1:
                continue
            new_subject = row1['subject_ids']
            new_user = row1['user_name']
            new_value = int(row1['value'])

            # test for a change in the selector - in this case the selector is the subject
            if new_subject != subject:
                if row_counter != 1:  # not the first time through
                    subjects += process_aggregation(subject, vote_counter, workflow_id,
                                                    workflow_version, value_total)
                # reset the selector, those things we need to pass through,
                # and the bins for the next aggregation.
                subject = new_subject
                user_name = {new_user}
                workflow_id = row1['workflow_id']
                workflow_version = row1['workflow_version']
                value_total = new_value
                vote_counter = 1
            else:
                # The selector has not yet changed so we continue the aggregation:
                if user_name != user_name | {new_user}:
                    value_total += new_value  # typical aggregation for a field which can be summed
                    vote_counter += 1
                user_name |= {new_user}
        # catch and process the last aggregated group
        subjects += process_aggregation(subject, vote_counter, workflow_id,
                                        workflow_version, value_total)

    print(row_counter, 'lines aggregated into ', subjects, ' subjects.')

print(not_found)
