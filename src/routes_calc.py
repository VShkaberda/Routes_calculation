# -*- coding: utf-8 -*-
"""
Created on Fri Jan 11 14:36:08 2019

@author: v.shkaberda
"""

from collections import defaultdict
from geoYN import geoYN

import argparse
import os.path, sys


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-v', '--version', action='store_true',
                        help='Show version')
    parser.add_argument('-c', '--count', action='store_true',
                        help='Count rows that have to be updated')
    parser.add_argument('server', nargs='?', type=str, choices=['s31', 's64'], default=0,
                        help='s31 - work with rva_GeoMatrix on s31;\n'
                        's64 - work with rva_GeoMatrixImport on s64')
    args = parser.parse_args()

    if args.version:
        from _version import __version__
        print(__version__)
        sys.exit(0)

    if not args.server:
        parser.print_help()
        sys.exit(1)

    # Initializaing parameters
    db_params = defaultdict(str)
    db_params['server'] = f's-kv-center-{args.server}'

    assert os.path.isfile('config.ini'), 'Error: config.ini doesn\'t exists'

    # Reading config and setting parameters
    try:
        with open('config.ini', 'r') as f:
            for line in f:
                if line.startswith(args.server):
                    k = line[line.index('.')+1:line.index(':')].strip()
                    v = line[line.index(':')+1:].strip()
                    db_params[k] = v

    except ValueError:
        print('Error: config.ini have unappropriate format:')
        print(line)
        sys.exit(1)

    if not db_params['db']:
        print('Error: Database name is not defined in config.ini')
        sys.exit(1)

    geoYN(args, db_params)


if __name__ == '__main__':
    main()
    input('End\nPress "Enter" to exit...')
