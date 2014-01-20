"""
Utility to manage and execute map-reduce jobs on Amazon EMR.

Written for the Rankmaniac competition (2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Max Hirschhorn (maxh@caltech.edu)
"""

from config import teams, datasets

jobs = []

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

if __name__ == '__main__':
    import argparse

    description = 'Manage and execute map-reduce jobs on Amazon EMR.'
    parser = argparse.ArgumentParser(description=description)

    subparsers = parser.add_subparsers()

    # Create the parser for the list command
    parser_list = subparsers.add_parser('list')
    parser_list.add_argument('kind', choices=['teams', 'jobs', 'datasets'])
    parser_list.set_defaults(func=handle_list)

    while True:
        try:
            line = raw_input('rankmaniac> ')
            args = parser.parse_args(line.split())
            args.func(args)
        except KeyboardInterrupt:
            print()
            break
        except:
            pass
