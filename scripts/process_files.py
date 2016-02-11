"""Initiate file analysis.

This module is used to find the discrepancies between two given files
"""
import argparse
import csv
import logging

import pandas as pd

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.DEBUG)


mathching_records_path = 'mathing_records.csv'
non_mathching_records_path = 'non_mathing_records.csv'
records_diff = "customers_in_chartio_but_not_in_responsys.csv"
no_project_key_path = 'chartio_records_with_no_project_key.csv'
no_customer_key_path = 'chartio_records_with_no_customer_key.csv'
no_project_status_path = 'chartio_records_with_no_project_status.csv'

CHARTIO_GRADES = ['exceeded', 'failed', 'passed', 'ungradeable']


def get_file_df(file_path):
    """Read the file from file path then create a Pandas data frame from file.

    It eventually extracts the needed keys from this df and stored it in a dict
    Args:
        file_path(str): This is the path to the csv file to be read
    Returns:
        target_dict(dict): This holds key value pairs for future comparison
    """
    logger.info("Reading CSV file.")
    contacts_df = pd.read_csv(file_path)
    target_dict = dict()
    for contact_info in contacts_df.itertuples():
        # Each unique_key is a concatenation of the contact_info's account_key
        # and the project_id.
        contact_id = contact_info[2]
        project_id = contact_info[1]
        unique_key = "{x}-{y}".format(x=contact_id, y=project_id)
        contact_tuple = {
            unique_key: contact_info
        }
        target_dict.update(contact_tuple)
    return target_dict


def write_to_csv(file_path, content):
    """Write content to file in file path.

    This simple method writes the given content to the file in the  specified
    file path.
    It creates a new file in the path if no file exists there
    It keeps appending to the file per call.
    Args:
        file_path(str): Path to file
        content(list): A list of records which represent each row of the file
    TODO: Make whole process run faster by opening each file during the
    whole process
    Opening and closing a file per call slows down the whole write process
    """
    with open(file_path, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(content)


def get_unique_key(project_status, project_id, customer_id):
    """Return unique key from given record.

    Returns:
        unique_key(str): This is the unique key which is used to search
        the dict which holds the overall records
    """
    project_key = project_id.replace('.0', '')
    customer_key = customer_id.replace('.0', '')
    record = [project_status, project_key, customer_key]
    invalid_result = project_status not in CHARTIO_GRADES
    invalid_project_key = project_key == 'nan'
    invalid_customer_key = customer_key == 'nan'
    if invalid_result or invalid_project_key or invalid_customer_key:
        if invalid_result and project_status == 'nan':
            record[0] = None
            write_to_csv(no_project_status_path, record)
            return False
        if project_key == 'nan':
            record[1] = None
            write_to_csv(no_project_key_path, record)
            return False
        elif customer_key == 'nan':
            record[2] = None
            write_to_csv(no_customer_key_path, record)
            return False
    else:
        unique_key = "{x}-{y}".format(x=customer_key, y=project_key)
        return unique_key


def translate_result(student_grade):
    """Interprete what a student_grade in one file means in another.

    Args:
        student_grade(str): a string which represents the grade the student
        in one file
    Returns:
        Student grade equivalent in another file
    """
    translator = {
        'ungradeable': ['INCOMPLETE', 'UNGRADED', 'SUBMITTED'],
        'failed': ['INCOMPLETE'],
        'passed': ['PASSED'],
        'exceeded': ['DISTINCTION']
    }
    return translator[student_grade]


def check_status(unique_key, project_status, keys_dict):
    """Compare two status against each other.

    Compares two statuses against each other and calls the appropriate function
    """
    result_list = translate_result(project_status)
    try:
        unique_record = keys_dict[unique_key]
        project_result = unique_record[3]
        if project_result in result_list:
            record = list(unique_record)[1:4]
            record.append(project_status)
            write_to_csv(mathching_records_path, record)
        else:
            record = list(unique_record)[1:4]
            record.append(project_status)
            write_to_csv(non_mathching_records_path, record)
    except (KeyError, ValueError, TypeError):
        account_project_keys = unique_key.split('-')
        record = [
            account_project_keys[0],
            account_project_keys[1],
            project_status
        ]
        write_to_csv(records_diff, record)


def compare_keys_with_files_in(file_path, keys_dict):
    """Go through a file and extract and processes its contents.

    This is one very shitty doc string
    """
    contacts_df = pd.read_csv(file_path)
    for contact_info in contacts_df.itertuples():
        index, project_status, project_key, customer_key = contact_info
        unique_key = get_unique_key(
            str(project_status),
            str(project_key),
            str(customer_key)
        )
        if unique_key:
            check_status(unique_key, str(project_status), keys_dict)


def main():
    """Run all scripts from here.

    This is the master script that initiates all the other scripts.
    """
    logger.info("Script started..")
    parser = argparse.ArgumentParser()
    parser.add_argument('first_file_path', help="Path to first CSV file.")
    parser.add_argument('second_file_path', help="Path to second CSV file.")
    args = parser.parse_args()
    account_project_keys = get_file_df(args.responsys_file_path)
    compare_keys_with_files_in(args.chartio_file_path, account_project_keys)


if __name__ == '__main__':
    main()
