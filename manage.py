"""
Utility to manage and execute map-reduce jobs on Amazon EMR.

Written for the Rankmaniac competition (2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Max Hirschhorn (maxh@caltech.edu)
"""

import sys, os
from time import gmtime, strftime, sleep

from boto.exception import EmrResponseError

from config import TEAMS, DATASETS, SCORE_SESSION

import scoreboard_client as scoreboard
from grader import Grader

####
## Assumptions
####
assert len(DATASETS) >= 0, 'need at least one dataset'
for dataset in DATASETS:
    if not os.path.isfile(os.path.join('data', dataset)):
        raise Exception('missing dataset %s' % (dataset))
    elif not os.path.isfile(os.path.join('sols', dataset)):
        raise Exception('missing solution for %s dataset' % (dataset))

unbuff_stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # unbuffered

jobs = []
job_ids_by_team_id = {}

infile = DATASETS[0]

def handle_list(args):
    """
    Handler for the `list` command.
    """

    # display the name of each team
    if args.key == 'teams':
        for team in TEAMS:
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
        for dataset in DATASETS:
            print(dataset)

def handle_get(args):
    """
    Handler for the `get` command.
    """

    if args.key == 'infile':
        if infile is not None:
            print(infile)

def handle_set(args):
    """
    Handler for the `set` command.
    """

    if args.key == 'infile':
        if args.value not in DATASETS:
            raise Exception('invalid dataset')
        global infile
        infile = args.value

def handle_run(args):
    """
    Handler for the `run` command.
    """

    team_ids = args.teams

    if len(team_ids) == 1 and team_ids[0] == '*':
        team_ids = TEAMS

    team_ids = list(team_ids) # make list so can be modified in loop

    # Check for any invalid teams or those with jobs already running
    unbuff_stdout.write('Validating')
    i = 0
    while i < len(team_ids): # length is NOT constant
        team_id = team_ids[i]
        unbuff_stdout.write('.')

        # Check that team name is valid
        if team_id not in TEAMS:
            if not args.suppress:
                raise Exception('invalid team %s' % (team_id))
            team_ids.pop(i)
            print('')
            print('SKIPPING! Invalid team %s.' % (team_id))

        # Check that team does not already have job running
        elif team_id in job_ids_by_team_id:
            if not args.suppress:
                raise Exception('team %s already running' % (team_id))
            team_ids.pop(i)
            print('')
            print('SKIPPING! Team %s already has a job running.' % (team_id))

        else:
            i += 1
    print('')

    # Do a second pass to spawn map-reduce jobs
    unbuff_stdout.write('Spawning')
    for team_id in team_ids:
        unbuff_stdout.write('.')
        g = Grader(team_id, infile)
        if g.do_setup():
            jobs.append(g)

            job_id = len(jobs) - 1
            job_ids_by_team_id[team_id] = job_id
        else:
            scoreboard.record_no_submission(team_id, SCORE_SESSION)
            print('')
            print('Team %s had no submission to run.' % (team_id))
    print('')

def handle_kill(args):
    """
    Handler for the `kill` command.
    """

    if args.key == 'team':
        team_id = args.team
        if team_id not in TEAMS or team_id not in job_ids_by_team_id:
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
    del job_ids_by_team_id[team_id]

def handle_time(args):
    """
    Handler for the `time` command.
    """

    if args.key == 'team':
        team_id = args.team
        if team_id not in TEAMS or team_id not in job_ids_by_team_id:
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
    team_ids = args.teams

    if len(team_ids) == 1 and team_ids[0] == '*':
        team_ids = TEAMS

    team_ids = list(team_ids) # make list so can be modified in loop

    # Check for any invalid teams or those with jobs already running
    unbuff_stdout.write('Validating')
    i = 0
    while i < len(team_ids): # length is NOT constant
        team_id = team_ids[i]
        unbuff_stdout.write('.')

        # Check that team name is valid
        if team_id not in TEAMS:
            if not args.suppress:
                raise Exception('invalid team %s' % (team_id))
            team_ids.pop(i)
            print('')
            print('SKIPPING! Invalid team %s.' % (team_id))

        # Check that team does not already have job running
        elif team_id not in job_ids_by_team_id:
            if not args.suppress:
                raise Exception('team %s has no job running' % (team_id))
            team_ids.pop(i)
            print('')
            print('SKIPPING! Team %s has no job running.' % (team_id))

        else:
            i += 1
    print('')

    print('Waiting for map-reduce job to finish...')
    print('  Use Ctrl-C to interrupt')
    # Do a second pass to wait on all map-reduce jobs
    try:
        i = 0
        while len(team_ids) > 0:
            team_id = team_ids[i]
            try:
                unbuff_stdout.write('.')
                job_id = job_ids_by_team_id[team_id]
                job = jobs[job_id]

                # Check if job is done
                if job.is_done():
                    team_ids.pop(i) # remove, since graded
                    runtime = job.compute_job_time()
                    penalty = job.compute_penalty()
                    scoreboard.record_submission_time(team_id, runtime, penalty,
                                                      SCORE_SESSION)
                    print('')
                    print("Team %s successfully wrote 'FinalRank'." % (team_id))
                    continue

                # ...or if job has finished
                elif not job.is_alive():
                    team_ids.pop(i) # remove, since graded
                    scoreboard.record_invalid_submission(team_id, SCORE_SESSION)
                    print('')
                    print("Team %s failed to write 'FinalRank'." % (team_id))
                    continue

                # Otherwise, check next team
                i += 1
                i %= len(team_ids)

                sleep(20) # call Amazon APIs infrequently
            except EmrResponseError:
                sleep(60) # call Amazon APIs infrequently

            # Swallow any exceptions, and skip team
            except Exception as e:
                team_ids.pop(i) # remove, since graded
                scoreboard.record_invalid_submission(team_id, SCORE_SESSION)
                print('')
                print('Team %s generated an exception.' % (team_id))
                print('ERROR! %s' % (e))
    except KeyboardInterrupt:
        print('')

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
    parser_get.add_argument('key', choices=['infile'],
                            help='the field to get')
    parser_get.set_defaults(func=handle_get)

    # Create the parser for the set command
    parser_set = subparsers.add_parser('set')
    parser_set.set_defaults(func=handle_set)
    subparsers_set = parser_set.add_subparsers(dest='key',
                                               help='the field to set')

    parser_set_infile = subparsers_set.add_parser('infile')
    parser_set_infile.add_argument('value', metavar='DATASET',
                                   help='the dataset to use')

    # Create the parser for the run command
    parser_run = subparsers.add_parser('run')
    parser_run.add_argument('teams', metavar='TEAM', nargs='+',
                            help='the team identifier')
    parser_run.add_argument('-q', '--suppress', dest='suppress',
                            action='store_true', default=False,
                            help='swallow exceptions')
    parser_run.set_defaults(func=handle_run)

    # Create the parser for the kill command
    parser_kill = subparsers.add_parser('kill')
    parser_kill.set_defaults(func=handle_kill)
    subparsers_kill = parser_kill.add_subparsers(dest='key')

    parser_kill_team = subparsers_kill.add_parser('team')
    parser_kill_team.add_argument('team', metavar='TEAM', choices=TEAMS,
                                  help='the team identifier')

    parser_kill_job = subparsers_kill.add_parser('job')
    parser_kill_job.add_argument('job', metavar='JOB', type=int,
                                 help='the job identifier')

    # Create the parser for the time command
    parser_time = subparsers.add_parser('time')
    parser_time.set_defaults(func=handle_time)
    subparsers_time = parser_time.add_subparsers(dest='key')

    parser_time_team = subparsers_time.add_parser('team')
    parser_time_team.add_argument('team', metavar='TEAM', choices=TEAMS,
                                  help='the team identifier')

    parser_time_job = subparsers_time.add_parser('job')
    parser_time_job.add_argument('job', metavar='JOB', type=int,
                                 help='the job identifier')

    # Create the parser for the grade command
    parser_grade = subparsers.add_parser('grade')
    parser_grade.add_argument('teams', metavar='TEAM', nargs='+',
                              help='the team identifier')
    parser_grade.add_argument('-q', '--suppress', dest='suppress',
                              action='store_true', default=False,
                              help='swallow exceptions')
    parser_grade.set_defaults(func=handle_grade)

    while True:
        try:
            line = raw_input('rankmaniac> ')
            args = parser.parse_args(line.split())
            args.func(args)
        except KeyboardInterrupt:
            print('')
            break
        except Exception as e:
            print('ERROR! %s' % (e))
        except:
            pass
