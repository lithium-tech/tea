#!/usr/bin/env python3

import csv
import json
import sys
import argparse
import os.path
import glob

COLUMN_SEVERITY = 16
COLUMN_MESSAGE = 18
COLUMN_FILE = 27


def is_stat_row(row):
    return row[COLUMN_FILE].startswith("tea_") and "event_type" in row[COLUMN_MESSAGE]


def is_open_readers_row(row):
    return row[COLUMN_FILE].startswith("tea_") and "open readers" in row[COLUMN_MESSAGE]


def get_segment_dirs(data_dir, segment_prefix):
    return [
        d
        for d in glob.glob(os.path.join(data_dir, segment_prefix + "*"))
        if os.path.isdir(d)
    ]


def get_last_log_file(segment_dir):
    log_files = [
        f
        for f in glob.glob(os.path.join(segment_dir, "*/pg_log/*.csv"))
        if os.path.isfile(f)
    ]
    log_files.sort()
    return log_files[-1]


def get_last_stat_message(log_file, encoding):
    message = None
    with open(log_file, newline="", encoding=encoding) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if is_stat_row(row):
                message = row[COLUMN_MESSAGE]
    return message

def ensure_no_open_readers_rows(log_file, encoding):
    with open(log_file, newline="", encoding=encoding) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            assert not is_open_readers_row(row)


def validate_message_format(msg):
    assert isinstance(msg["event_type"], str)
    assert isinstance(msg["session_id"], str)
    assert isinstance(msg["version"], str)
    assert msg["total_duration_seconds"] >= 0
    assert msg["plan_duration_seconds"] >= 0
    assert msg["fetch_duration_seconds"] >= 0
    assert msg["filter_build_seconds"] >= 0
    assert msg["filter_apply_seconds"] >= 0
    assert msg["read_duration_seconds"] >= 0
    assert msg["heap_form_tuple_seconds"] >= 0
    assert msg["s3_requests"] >= 0
    assert (
        msg["total_duration_seconds"]
        >= msg["plan_duration_seconds"] +
        msg["fetch_duration_seconds"] +
        msg["convert_duration_seconds"] +
        msg["filter_build_seconds"] +
        msg["filter_apply_seconds"] +
        msg["read_duration_seconds"] +
        msg["heap_form_tuple_seconds"]
    )
    assert msg["data_files_planned"] >= 0
    assert (
        msg["total_files_read"]
        >= msg["data_files_read"]
        + msg["positional_delete_files_read"]
        + msg["equality_delete_files_read"]
    )
    assert msg["data_files_read"] >= 0
    assert msg["row_groups_read"] >= 0
    assert msg["row_groups_skipped_filter"] >= 0
    assert msg["rows_read"] >= 0
    assert msg["rows_skipped_filter"] >= 0
    assert msg["rows_skipped_equality_delete"] >= 0
    assert msg["rows_skipped_positional_delete"] >= 0
    assert msg["retry_count"] >= 0
    assert msg["columns_for_greenplum"] >= 0
    assert msg["columns_equality_delete"] >= 0
    assert msg["columns_only_for_equality_delete"] >= 0
    assert msg["columns_read"] >= 0
    assert msg["bytes_read_from_s3"] >= 0

    assert msg["positional_delete_files_read"] >= 0
    assert msg["positional_delete_rows_read"] >= 0
    assert msg["equality_delete_files_read"] >= 0
    assert msg["equality_delete_rows_read"] >= 0
    assert msg["equality_delete_max_rows_materialized"] >= 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-D", "--data-dir", default="/gpdata")
    parser.add_argument("-s", "--segment-prefix", default="s")
    parser.add_argument("-e", "--encoding", default="cp1251")
    args = parser.parse_args()

    for d in get_segment_dirs(args.data_dir, args.segment_prefix):
        try:
            log = get_last_log_file(d)
            message = get_last_stat_message(log, args.encoding)
            ensure_no_open_readers_rows(log, args.encoding)
            validate_message_format(json.loads(message))
        except Exception as e:
            print("Stat message check failed", file=sys.stderr)
            if log:
                print("Log File: ", log, file=sys.stderr)
            else:
                print("Dir: ", d, file=sys.stderr)
            if message:
                print("Message: ", message, file=sys.stderr)
            raise


if __name__ == "__main__":
    main()
