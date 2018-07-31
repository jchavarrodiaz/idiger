"""
This tool can be used to mirror files from a source directory to a destination
directory. You can specify one file using `destination` and `source` or define
many files using `source_map` (a csv with source,destination file per line).
"""

import argparse
import os
import time

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Listen for file changes and mirror changed files to a second location.')
    parser.add_argument('-s', '--source', dest='source', action='store', default=None, help='The source file', required=False)
    parser.add_argument('-d', '--destination', dest='destination', action='store', default=None, help='The destination file', required=False)
    parser.add_argument('-m', '--source_map', dest='source_map', action='store', default=None, help='A CSV file mapping multiple source files to multiple targets', required=False)
    args = parser.parse_args()

    if not (args.source_map or (args.source and args.destination)):
        raise ValueError("You must provide either a source_map of files or a source and destination file")

    file_list = []

    if args.source and args.destination:
        file_list.append((args.source, args.destination,))

    if args.source_map:
        source_map = os.path.normpath(args.source_map)
        with open(source_map, 'rb') as f:
            for line in f.readlines():
                file_list.append(tuple(line.strip().split(",")))

    last_checked_map = {}

    # currently only ctrl+c will terminate
    while True:
        for t in file_list:
            source_file = os.path.normpath(t[0])
            destination_file = os.path.normpath(t[1])
            try:
                stat = os.stat(source_file)
            except OSError as e:
                print "Encountered a OSError, skipping file:", e
                continue
            last_time = last_checked_map.get(source_file)

            if not last_time or stat.st_mtime > last_time:
                f = open(source_file, 'rb')
                filedata = f.read()
                f.close()
                with open(destination_file, 'w') as f:
                    f.writelines(filedata)
                last_checked_map[source_file] = stat.st_mtime
                print "File %s changed, updated %s" % (source_file, destination_file)

        time.sleep(1)
