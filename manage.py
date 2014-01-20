"""
Utility to manage and execute map-reduce jobs on Amazon EMR.

Written for the Rankmaniac competition (2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Max Hirschhorn (maxh@caltech.edu)
"""

from boto.exception import EmrResponseError

from config import S3_GRADING_BUCKET
from config import teams, datasets

from grader import Grader

assert len(datasets) >= 0

jobs = []
job_ids_by_team_id = {}

bucket = S3_GRADING_BUCKET
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
            if r is not None:
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

def handle_run(args):
    """
    Handler for the `run` command.
    """

    team_id = args.team
    if team_id not in teams:
        raise Exception('invalid team')

    if team_id not in job_ids_by_team_id:
        g = Grader(team_id, infile)
        if g.do_setup():
            jobs.append(g)

            job_id = len(jobs) - 1
            job_ids_by_team_id[team_id] = job_id
        else:
            print('No submission to run.')

def handle_kill(args):
    """
    Handler for the `kill` command.
    """

    if args.kind == 'team':
        team_id = args.team
        if team_id not in teams or team_id not in job_ids_by_team_id:
            raise Exception('invalid team')
        job_id = job_ids_by_team_id[team_id]

    elif args.kind == 'job':
        job_id = args.job
        if job_id >= len(jobs):
            raise Exception('invalid job')

    job = jobs[job_id]
    if job is None:
        raise Exception('job already terminated')

    jobs[job_id] = None
    del job

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

    # Create the parser for the run command
    parser_run = subparsers.add_parser('run')
    parser_run.add_argument('team', choices=teams)
    parser_run.set_defaults(func=handle_run)

    # Create the parser for the kill command
    parser_kill = subparsers.add_parser('kill')
    parser_kill.set_defaults(func=handle_kill)
    subparsers_kill = parser_kill.add_subparsers(dest='kind')

    parser_kill_team = subparsers_kill.add_parser('team')
    parser_kill_team.add_argument('team', metavar='TEAM', choices=teams,
                                  help='the team identifier')

    parser_kill_job = subparsers_kill.add_parser('job')
    parser_kill_job.add_argument('job', metavar='JOB', type=int,
                                 help='the job identifier')

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
