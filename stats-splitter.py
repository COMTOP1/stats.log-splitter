import argparse
import os
import re
import sys

stats_file = 'stats.log'  # the stats file to parse
file_append = 'a'  # symbol to indicate an append to a file
file_read = 'r'  # symbol to indicate a read to a file
file_write = 'w'  # symbol to indicate a write over to a file
bucket_stats_folder_path = 'bucket_stats'  # base folder to save the parsed files to
bucket_folder_path = 'bucket'  # folder to store the bucket seperated files in
section_regex = r'(=+\n.+\n.+\n=+)'  # regex to identify the section break
bucket_regex = r'(\*+\n.+\n)'  # regex to identify the bucket break
star_isolate_regex = r'(\*+\n)'  # regex to parse out the stars above the bucket name
# where the contents of the files will be stored before writing the file
# the format of this file is [<bucket name>][<section name>] = <contents>
bucket_files_contents = {}
verbose = False
exclude_small = False
exclude_large = False


# parse_section_title parses the title and removes characters that can't be used
def parse_section_title(title):
    title = title.replace(' ', '_')  # replacing space with underscore
    title = title.replace('(', '')  # replacing opening bracket with empty space
    title = title.replace(')', '')  # replacing closing bracket with empty space
    title = title.replace('\'', '')  # replacing single quote with empty space
    title = title.replace(',', '')  # replacing comma with empty space
    title = title.replace('_[]', '')  # replacing the special underscore then empty square brackets with empty space
    title = title.replace('[', '')  # replacing opening square bracket with empty space
    title = title.replace(']', '')  # replacing closing square bracket with empty space
    return title


# handle_dict creates a dictionary in a dictionary if it doesn't exist
def handle_dict(bucket_name):
    if bucket_name not in bucket_files_contents:
        bucket_files_contents[bucket_name] = {}
    return


# mkdir creates a directory if it doesn't exist
def mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)
        if verbose:
            print(f'mkdir: {path}')
    return


# split_file_into_sections splits the raw file into the different sections then parses all the sections and buckets
def split_file_into_sections():
    path = os.path.join(stats_file)
    if verbose:
        print(f'opening file in read mode: {path}')
    raw_file = open(
        path, file_read
    ).read()
    section_matches = re.findall(section_regex, raw_file)

    section_count = 0
    raw_file_split_by_section = re.split(section_regex, raw_file)
    for section_match_raw in section_matches:
        handle_section_match(section_match_raw, raw_file_split_by_section, section_count)
        section_count = section_count + 1
    return


# handle_section_match handles the raw sections and either writes to non-bucket file or to the bucket dictionary
def handle_section_match(section_match_raw, raw_file_split, section_count):
    section_match_raw_split = str.splitlines(section_match_raw)
    section_title = parse_section_title(section_match_raw_split[1])

    bucket_matches = re.findall(bucket_regex, raw_file_split[(section_count * 2) + 2])
    if len(bucket_matches) > 0:
        bucket_file_add_contents(raw_file_split, bucket_matches, section_title, section_count)
    else:
        header = raw_file_split[(section_count * 2) + 1]
        contents = raw_file_split[(section_count * 2) + 2]
        write_non_bucket_file(section_title, header, contents)
    return


# bucket_file_add_contents adds the contents of the bucket sections to the bucket dictionary
def bucket_file_add_contents(section_split, bucket_matches, section_title, section_count):
    bucket_split = re.split(bucket_regex, section_split[(section_count * 2) + 2])
    bucket_count = 0
    for bucket_match_raw in bucket_matches:
        bucket_name_split = re.split(star_isolate_regex, str(bucket_match_raw).rstrip())
        bucket_name = bucket_name_split[2]
        handle_dict(bucket_name)
        header = section_split[(section_count * 2) + 1]
        contents = bucket_split[(bucket_count * 2) + 2]
        bucket_files_contents[bucket_name][section_title] = f'{header}\n{bucket_name}\n{contents}'
        bucket_count = bucket_count + 1
    return


# write_non_bucket_file writes to a file where there are no bucket separation within the section
def write_non_bucket_file(log_file_name, header, body):
    path = os.path.join(bucket_stats_folder_path, f'{log_file_name}.log')
    if verbose:
        print(f'opening/making file in write mode: {path}')
    non_bucket_file = open(
        path, file_write
    )
    non_bucket_file.write(header)
    non_bucket_file.write(body)
    non_bucket_file.close()
    return


# write_from_dict_contents creates the collated files for each bucket with each of its sections within
def write_from_dict_contents():
    mkdir(os.path.join(bucket_stats_folder_path, bucket_folder_path))
    for bucket_name, bucket_sections in bucket_files_contents.items():
        if not exclude_small:
            mkdir(
                os.path.join(bucket_stats_folder_path, bucket_folder_path, bucket_name)
            )
        path = os.path.join(bucket_stats_folder_path, bucket_folder_path, f'{bucket_name}.log')
        if not exclude_large:
            if verbose:
                print(f'opening/making file in append mode: {path}')
            collated_file = (
                open(
                    path, file_append
                )
            )
            write_from_dict_inner_contents(bucket_sections, bucket_name, collated_file)
            collated_file.close()
        else:
            write_from_dict_inner_contents_no_collated(bucket_sections, bucket_name)
    return


# write_from_dict_inner_contents creates individual files within the bucket folder of the seperated sections
def write_from_dict_inner_contents(bucket_sections, bucket_name, collated_file):
    for file_name, file_content in bucket_sections.items():
        if not exclude_small:
            path = os.path.join(bucket_stats_folder_path, bucket_folder_path, bucket_name, f'{file_name}.log')
            if verbose:
                print(f'opening/making file in write mode: {path}')
            individual_file = (
                open(
                    path, file_write
                )
            )
            individual_file.write(file_content)
            individual_file.close()
        collated_file.write(file_content)
        collated_file.write('\n')
    return


# write_from_dict_inner_contents_no_collated creates individual files within the bucket folder of the seperated sections
# without the collated file
def write_from_dict_inner_contents_no_collated(bucket_sections, bucket_name):
    for file_name, file_content in bucket_sections.items():
        path = os.path.join(bucket_stats_folder_path, bucket_folder_path, bucket_name, f'{file_name}.log')
        if verbose:
            print(f'opening/making file in write mode: {path}')
        individual_file = (
            open(
                path, file_write
            )
        )
        individual_file.write(file_content)
        individual_file.close()
    return


# main method that is executed at runtime
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='stats-splitter.py',
        description='This program splits out the massive stats.log file that is collected by cbcollect ' +
                    'and can parse into multiple smaller files.\nBy default, the normal collection method is to ' +
                    'split the file into the sections without buckets and then in the bucket folders then make ' +
                    'individual files for each section.')
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='outputs the log file names that are being written and all folders being made')
    parser.add_argument('-s', '--no-small',
                        action='store_true',
                        help='excludes the small individual files from the bucket folders ' +
                             'and only has larger bucket files, can\'t be used with -l')
    parser.add_argument('-l', '--no-large',
                        action='store_true',
                        help='excludes the larger collated files from the buckets folders ' +
                             'and only has smaller bucket section files, can\'t be used with -s')

    args = parser.parse_args()
    if args.no_small & args.no_large:
        print('invalid argument options, cannot use -s and -l together')
        sys.exit(1)
    exclude_large = args.no_large
    exclude_small = args.no_small
    verbose = args.verbose
    mkdir(
        os.path.join(bucket_stats_folder_path)
    )
    split_file_into_sections()
    write_from_dict_contents()
