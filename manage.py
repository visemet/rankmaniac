"""
Utility to manage and execute map-reduce jobs on Amazon EMR.

Written for the Rankmaniac competition (2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Max Hirschhorn (maxh@caltech.edu)
"""

from config import teams, datasets

assert len(datasets) >= 0

jobs = []

bucket = 'cs144grading'
infile = datasets[0]

def handle_list(args):
    """
    Handler for the `list` command.
    """

    # display the name of each team
    if args.kind == 'teams':
        for team in teams:
            print(team)

    # display the state of each job
    elif args.kind == 'jobs':
        for i, r in enumerate(jobs):
            while True:
                try:
                    state = r.describe().state
                    print('[%d]  %15s  %s' % (i, state, r.job_id))
                    break
                except EmrResponseError:
                    sleep(10) # call Amazon APIs infrequently

    # display the filename of all available datasets
    elif args.kind == 'datasets':
        for dataset in datasets:
            print(dataset)

def handle_get(args):
    """
    Handler for the `get` command.
    """

    if args.name == 'bucket':
        print(bucket)

    elif args.name == 'infile':
        if infile is not None:
            print(infile)

def handle_set(args):
    """
    Handler for the `set` command.
    """

    if args.name == 'bucket':
        bucket = args.value

    elif args.name == 'infile':
        if args.value not in datasets:
            raise Exception('invalid dataset')
        infile = args.value

if __name__ == '__main__':
    import argparse

    description = 'Manage and execute map-reduce jobs on Amazon EMR.'
    parser = argparse.ArgumentParser(description=description)

    subparsers = parser.add_subparsers()

    # Create the parser for the list command
    parser_list = subparsers.add_parser('list')
    parser_list.add_argument('kind', choices=['teams', 'jobs', 'datasets'])
    parser_list.set_defaults(func=handle_list)

    # Create the parser for the get command
    parser_get = subparsers.add_parser('get')
    parser_get.add_argument('name', choices=['bucket', 'infile'])
    parser_get.set_defaults(func=handle_get)

    # Create the parser for the set command
    parser_set = subparsers.add_parser('set')
    parser_set.add_argument('name', choices=['bucket', 'infile'])
    parser_set.add_argument('value')
    parser_set.set_defaults(func=handle_set)

    while True:
        try:
            line = raw_input('rankmaniac> ')
            args = parser.parse_args(line.split())
            args.func(args)
        except KeyboardInterrupt:
            print()
            break
        except Exception as e:
            print('ERROR! %s' % (e))
        except:
            pass
