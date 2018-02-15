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
location = r'C:\py\ZooGems\radio-galaxy-zoogems-classifications14Feb0945.csv'
target_location = r'C:\py\ZooGems\RGZGtargets.csv'
out_location = r'C:\py\ZooGems\radio-galaxy-zoogems_flattened.csv'
sorted_location = r'C:\py\ZooGems\radio-galaxy-zoogems_sorted.csv'
aggregate_location = r'C:\py\ZooGems\radio-galaxy-zoogems_aggregated.csv'
sorted_aggregate = r'C:\py\ZooGems\radio-galaxy-zoogems_ranked.csv'


# Function definitions needed for any blocks.


def include(class_record):
    #  define a function that returns True or False based on whether the argument record is to be included or not in
    #  the output file based on the conditional clauses.  
    if class_record['user_name'] == 'Jean Tate':
        return False
    elif int(class_record['workflow_id']) == 5916 and int(class_record['classification_id']) >= 88599078:
        return True
    elif int(class_record['workflow_id']) == 5973 and int(class_record['classification_id']) >= 88599030:
        return True
    elif int(class_record['workflow_id']) == 5981 and int(class_record['classification_id']) >= 88600446:
        return True
    elif int(class_record['workflow_id']) == 5984 and int(class_record['classification_id']) >= 88600741:
        return True
    elif int(class_record['workflow_id']) == 5985 and int(class_record['classification_id']) >= 88601073:
        return True
    elif int(class_record['workflow_id']) == 5986 and int(class_record['classification_id']) >= 88601447:
        return True
    elif int(class_record['workflow_id']) == 5987 and int(class_record['classification_id']) >= 88601857:
        return True
    elif int(class_record['workflow_id']) == 5989 and int(class_record['classification_id']) >= 88600090:
        return True
    elif int(class_record['workflow_id']) == 5990 and int(class_record['classification_id']) >= 88602613:
        return True
    elif int(class_record['workflow_id']) == 5991 and int(class_record['classification_id']) >= 88602907:
        return True
    elif int(class_record['workflow_id']) == 6004 and int(class_record['classification_id']) >= 88793313:
        return True
    elif int(class_record['workflow_id']) == 6006 and int(class_record['classification_id']) >= 88793731:
        return True
    else:
        return False


# opens the RGZGtargets.csv file and builds a dictionary of the ID's and locations
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

    # this area for initializing counters, 
    i = 0
    j = 0
   
    #  open the zooniverse data file using dictreader,  and load the more complex json strings as python objects
    with open(location) as f:
        r = csv.DictReader(f)
        for row in r:
            i += 1  # counts rows         
            if include(row) is True:
                j += 1  #  counts included rows 
                annotations = json.loads(row['annotations'])
                
                # flatten First task and assign values
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
        average_ranking = round(value_tot / vote_count, 2)
        try:
            new_row = {'Set': target[subj]['Set'],
                       'Jname': target[subj]['Jname'],
                       'RA': target[subj]['RA'],
                       'Dec': target[subj]['Dec'],
                       'SDSS_ObjId': json.dumps(target[subj]['SDSS_ObjId']),
                       'subject_ids': subj,
                       'total_votes': vote_count,
                       'workflow_id': workflow,
                       'workflow_version': version,
                       'average ranking': average_ranking}
        except KeyError:
            not_found.append(subj)
            new_row = {'Set': '',
                       'Jname': '',
                       'RA': '',
                       'Dec': '',
                       'SDSS_ObjId': '',
                       'subject_ids': subj,
                       'total_votes': vote_count,
                       'workflow_id': workflow,
                       'workflow_version': version,
                       'average ranking': average_ranking}
        writer.writerow(new_row)
        return 1
    else:
        return 0


# set up the output file names for the aggregated and processed data, and write the header.
with open(aggregate_location, 'w', newline='') as file:
    fieldnames = ['subject_ids',
                  'total_votes',
                  'workflow_id',
                  'workflow_version',
                  'Set',
                  'Jname',
                  'RA',
                  'Dec',
                  'SDSS_ObjId',
                  'average ranking'
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
        vote_counter = 0
        value_total = 0
        row_counter = 0
        subjects = 0
        not_found = []

        # Loop over the flattened classification records
        for row1 in r:
            row_counter += 1
            # read a row and pullout the flattened data fields we need to aggregate, or pass through.
            new_subject = row1['subject_ids']
            new_user_ip = row1['user_ip']
            new_value = int(row1['value'])

            # test for a change in the selector - in this case the selector is the subject
            if new_subject != subject:
                if vote_counter != 1:  #  not the first time through
                    subjects += process_aggregation(subject, vote_counter, workflow_id,
                                                    workflow_version, value_total)
                # reset the selector, those things we need to pass through,
                # and the bins for the next aggregation.
                subject = new_subject
                user_ip = {new_user_ip}
                workflow_id = row1['workflow_id']
                workflow_version = row1['workflow_version']
                value_total = new_value
                vote_counter = 1

            else:
                # The selector has not yet changed so we continue the aggregation:
                if user_ip != user_ip | {new_user_ip} and new_value > 0:
                    user_ip |= {new_user_ip}
                    value_total += new_value  # typical aggregation for a field which can be summed
                    vote_counter += 1

        # catch and process the last aggregated group
        subjects += process_aggregation(subject, vote_counter, workflow_id,
                                        workflow_version, value_total)

    print(row_counter, 'lines aggregated into ', subjects, ' subjects.')

print(sort_file(aggregate_location, sorted_aggregate, 9, True, False), 'lines sorted and written')
print(not_found)
