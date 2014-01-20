"""
Utility to manage and execute map-reduce jobs on Amazon EMR.

Written for the Rankmaniac competition (2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Max Hirschhorn (maxh@caltech.edu)
"""

import sys
from time import gmtime, strftime, sleep

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
    if args.key == 'teams':
        for team in teams:
            print(team)

    # display the state of each job
    elif args.key == 'jobs':
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
    elif args.key == 'datasets':
        for dataset in datasets:
            print(dataset)

def handle_get(args):
    """
    Handler for the `get` command.
    """

    if args.key == 'bucket':
        print(bucket)

    elif args.key == 'infile':
        if infile is not None:
            print(infile)

def handle_set(args):
    """
    Handler for the `set` command.
    """

    if args.key == 'bucket':
        bucket = args.value

    elif args.key == 'infile':
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

    if args.key == 'team':
        team_id = args.team
        if team_id not in teams or team_id not in job_ids_by_team_id:
            raise Exception('invalid team')
        job_id = job_ids_by_team_id[team_id]

    elif args.key == 'job':
        job_id = args.job
        if job_id >= len(jobs):
            raise Exception('invalid job')

    job = jobs[job_id]
    if job is None:
        raise Exception('job already terminated')

    job.terminate() # force terminate to be called
    jobs[job_id] = None
    del job # clean up the rest

def handle_time(args):
    """
    Handler for the `time` command.
    """

    if args.key == 'team':
        team_id = args.team
        if team_id not in teams or team_id not in job_ids_by_team_id:
            raise Exception('invalid team')
        job_id = job_ids_by_team_id[team_id]

    elif args.key == 'job':
        job_id = args.job
        if job_id >= len(jobs):
            raise Exception('invalid job')

    job = jobs[job_id]
    if job is None:
        raise Exception('job already terminated')

    print(strftime('%H:%M:%S', gmtime(job.compute_job_time())))

def handle_grade(args):
    """
    Handler for the `grade` command.
    """

    if args.key == 'team':
        team_id = args.team
        if team_id not in teams or team_id not in job_ids_by_team_id:
            raise Exception('invalid team')
        job_id = job_ids_by_team_id[team_id]

    elif args.key == 'job':
        job_id = args.job
        if job_id >= len(jobs):
            raise Exception('invalid job')

    job = jobs[job_id]
    if job is None:
        raise Exception('job already terminated')

    print('Waiting for map-reduce job to finish...')
    print('  Use Ctrl-C to interrupt')
    while True:
        try:
            sys.stdout.write('.')
            if job.is_done():
                # TODO: record execution time and penalty on scoreboard
                break
            elif not job.is_alive():
                # TODO: record failed submission on scoreboard
                print()
                print("Failed to output 'FinalRank'!")
            sleep(20) # call Amazon APIs infrequently
        except EmrResponseError:
            sleep(60) # call Amazon APIs infrequently
        except KeyboardInterrupt:
            print()
            break
    print()

if __name__ == '__main__':
    import argparse

    desc = 'Manage and execute map-reduce jobs on Amazon EMR.'
    parser = argparse.ArgumentParser(description=desc)

    subparsers = parser.add_subparsers()

    # Create the parser for the list command
    parser_list = subparsers.add_parser('list')
    parser_list.add_argument('key', choices=['teams', 'jobs', 'datasets'],
                             help='the list to read')
    parser_list.set_defaults(func=handle_list)

    # Create the parser for the get command
    parser_get = subparsers.add_parser('get')
    parser_get.add_argument('key', choices=['bucket', 'infile'],
                            help='the field to get')
    parser_get.set_defaults(func=handle_get)

    # Create the parser for the set command
    parser_set = subparsers.add_parser('set')
    parser_set.set_defaults(func=handle_set)
    subparsers_set = parser_set.add_subparsers(dest='key',
                                               help='the field to set')

    parser_set_bucket = subparsers_set.add_parser('bucket')
    parser_set_bucket.add_argument('value', metavar='BUCKET',
                                   help='the bucket name')

    parser_set_infile = subparsers_set.add_parser('infile')
    parser_set_infile.add_argument('value', metavar='DATASET',
                                   help='the dataset to use')

    # Create the parser for the run command
    parser_run = subparsers.add_parser('run')
    parser_run.add_argument('team', choices=teams)
    parser_run.set_defaults(func=handle_run)

    # Create the parser for the kill command
    parser_kill = subparsers.add_parser('kill')
    parser_kill.set_defaults(func=handle_kill)
    subparsers_kill = parser_kill.add_subparsers(dest='key')

    parser_kill_team = subparsers_kill.add_parser('team')
    parser_kill_team.add_argument('team', metavar='TEAM', choices=teams,
                                  help='the team identifier')

    parser_kill_job = subparsers_kill.add_parser('job')
    parser_kill_job.add_argument('job', metavar='JOB', type=int,
                                 help='the job identifier')

    # Create the parser for the time command
    parser_time = subparsers.add_parser('time')
    parser_time.set_defaults(func=handle_time)
    subparsers_time = parser_time.add_subparsers(dest='key')

    parser_time_team = subparsers_time.add_parser('team')
    parser_time_team.add_argument('team', metavar='TEAM', choices=teams,
                                  help='the team identifier')

    parser_time_job = subparsers_time.add_parser('job')
    parser_time_job.add_argument('job', metavar='JOB', type=int,
                                 help='the job identifier')

    # Create the parser for the grade command
    parser_grade = subparsers.add_parser('grade')
    parser_grade.set_defaults(func=handle_grade)
    subparsers_grade = parser_grade.add_subparsers(dest='key')

    parser_grade_team = subparsers_grade.add_parser('team')
    parser_grade_team.add_argument('team', metavar='TEAM', choices=teams,
                                  help='the team identifier')

    parser_grade_job = subparsers_grade.add_parser('job')
    parser_grade_job.add_argument('job', metavar='JOB', type=int,
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
