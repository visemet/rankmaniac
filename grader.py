"""
Library for grading submissions.

Special notes:
    WARNING! Requires Python >= 2.7

Written for the Rankmaniac competition (2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Max Hirschhorn (maxh@caltech.edu)
"""

import os
import ConfigParser
from dateutil.parser import parse as dtparse
from time import sleep

from boto.exception import EmrResponseError
from boto.s3.key import Key

from config import AWS_ACCESS_KEY, AWS_SECRET_KEY
from config import S3_STUDENTS_BUCKET, S3_GRADING_BUCKET
from rankmaniac import Rankmaniac

class Grader(Rankmaniac):
    """
    (derived class)

    Adds more functionality to the rankmaniac.Rankmaniac class in order
    to be able to compute the time duration of the map-reduce job.
    """

    def __init__(self, team_id, infile, max_iter=50, num_instances=10):
        """
        (constructor)

        Creates a new instance of the Grader class for a specific team
        using the credentials from the project configuration.
        """

        Rankmaniac.__init__(self, team_id, AWS_ACCESS_KEY, AWS_SECRET_KEY,
                            bucket=S3_GRADING_BUCKET)

        self.set_infile(infile)

        self._max_iter = max_iter
        self._num_instances=num_instances

    def do_setup(self):
        """
        Submits a new map-reduce job to Amazon EMR and adds the maximum
        number of steps. Reads from the configuration from Amazon S3,
        but assumes if one does not exist, then no submission has been
        made!
        """

        # Default number of map and reduce tasks to execute
        num_mappers = 1
        num_reducers = 1

        # Default modules for where to expect the pagerank step
        # and process step code
        pagerank_map = 'pagerank_map.py'
        pagerank_reduce = 'pagerank_reduce.py'
        process_map = 'process_map.py'
        process_reduce = 'process_reduce.py'

        if self._copy_keys(S3_STUDENTS_BUCKET, S3_GRADING_BUCKET):
            # Download the configuration from the student bucket
            filename = '%s.cfg' % (self.team_id)
            if self._download_config(filename):
                # Upload the specified dataset to the grading bucket
                bucket = self._s3_conn.get_bucket(S3_GRADING_BUCKET)
                relpath = os.path.join('data', self._infile)
                if os.path.isfile(relpath): # also checks if file exists
                    keyname = self._get_keyname(self._infile)
                    key = bucket.new_key(keyname)
                    key.set_contents_from_filename(relpath)

                # Read the configuration and override defaults
                config = ConfigParser.SafeConfigParser()
                config.read(filename)

                section = 'Rankmaniac'
                if config.has_section(section):
                    num_mappers = config.getint(section, 'num_mappers')
                    num_reducers = config.getint(section, 'num_reducers')
                    pagerank_map = config.get(section, 'pagerank_map')
                    pagerank_reduce = config.get(section, 'pagerank_reduce')
                    process_map = config.get(section, 'process_map')
                    process_reduce = config.get(section, 'process_reduce')

                for i in range(self._max_iter):
                    while True:
                        try:
                            self.do_iter(pagerank_map, pagerank_reduce,
                                         process_map, process_reduce,
                                         num_pagerank_mappers=num_mappers,
                                         num_pagerank_reducers=num_reducers)
                            break
                        except EmrResponseError:
                            sleep(10) # call Amazon APIs infrequently

                os.remove(filename)
                return True

        return False # signal set-up has failed

    def compute_job_time(self, jobdesc=None):
        """
        Returns the sum of the amount of time, in seconds, each step of
        the map-reduce job took.

        Keyword arguments:
            jobdesc     <boto.emr.JobFlow>      cached description of
                                                jobflow to use
        """

        return sum(self._compute_step_times(jobdesc=jobdesc))

    def compute_penalty(self, multiplier=30, num_rank=20, max_diff=1000,
                        jobdesc=None):
        """
        Returns the amount of time, in seconds, to be added to the
        execution time as penalty for inaccuracy of results.

        Keyword arguments:
            jobdesc     <boto.emr.JobFlow>      cached description of
                                                jobflow to use
        """

        if not self.is_done(jobdesc=jobdesc):
            return None # not finished yet, so penalty undefined

        # Loads the solutions from the local directory
        sols = []
        with open(os.path.join('sols', self._infile), 'r') as f:
            for line in f:
                sols.append(int(line.strip()))

        assert len(sols) >= num_rank, (
            'must have at least top %d nodes in solution for %s dataset'
            % (num_rank, self._infile)
        )

        assert len(sols) <= max_diff, (
            'should have at most top %d nodes in solution for %s dataset'
            % (max_diff, self._infile)
        )

        # Retrieves the student rankings from Amazon S3
        i = self._last_process_step_iter_no
        outdir = self._get_default_outdir('process', iter_no=i)
        keyname = self._get_keyname(outdir, 'part-00000')

        bucket = self._s3_conn.get_bucket(S3_GRADING_BUCKET)
        key = Key(bucket=bucket, name=keyname)
        contents = key.get_contents_as_string()

        # Pad the list with enough values to make sure there are at
        # least num_rank nodes listed
        ranks = [-1] * num_rank # -1 is not a real node identifier
        for index, line in enumerate(contents.splitlines()):
            ranks.insert(index, int(line.split('\t')[1]))

        sum_sq_diff = 0
        for (actual, node) in enumerate(ranks[:num_rank]):
            try:
                expected = sols.index(node)
                sum_sq_diff += (actual - expected) ** 2

            # Note that no exception occurs assuming that the solution
            # contains the ranking for all of the nodes
            except ValueError:
                # However, if an exception does occur due to malformed
                # or lame input from the submission, then we apply
                # a large (enough) penalty
                sum_sq_diff += max_diff ** 2

        return multiplier * sum_sq_diff

    def _copy_keys(self, source_bucket_name, dest_bucket_name):
        """
        Copies the keys of a particular team from the source bucket to
        the destination bucket.
        """

        num_keys = 0

        source_bucket = self._s3_conn.get_bucket(source_bucket_name)
        dest_bucket = self._s3_conn.get_bucket(dest_bucket_name)

        # Clear out grading bucket/output contents for team
        keys = dest_bucket.list(prefix=self._get_keyname())
        dest_bucket.delete_keys(keys)

        keys = source_bucket.list(prefix='%s/' % (self.team_id))
        for key in keys:
            keyname = key.name
            suffix = keyname.split('/', 1)[1] # removes team identifier
            if '/' not in suffix and '$' not in suffix:
                key.copy(dest_bucket, self._get_keyname(suffix))
                num_keys += 1

        # Return whether anything was copied, i.e. previously submitted
        return num_keys > 0

    def _download_config(self, filename='rankmaniac.cfg'):
        """
        Downloads the configuration file for the given team. Returns
        `True` if the key was downloaded, and `False` otherwise.
        """

        bucket = self._s3_conn.get_bucket(S3_GRADING_BUCKET)
        keyname = self._get_keyname('rankmaniac.cfg')
        key = bucket.get_key(keyname)

        if key is not None:
            key.get_contents_to_filename(filename)
            return True

        return False # signal key did not exist

    def _compute_step_times(self, jobdesc=None):
        """
        Returns the amount of time, in seconds, each step of the
        map-reduce job took as a list.

        Keyword arguments:
            jobdesc     <boto.emr.JobFlow>      cached description of
                                                jobflow to use
        """

        if jobdesc is None:
            jobdesc = self.describe()

        self.is_done(jobdesc=jobdesc) # make sure that last process step
                                      # iteration number is up to date

        max_step = 0
        if self._last_process_step_iter_no >= 0:
            max_step = 2 * self._last_process_step_iter_no + 1

        times = []
        steps = jobdesc.steps

        i = 0
        while i <= max_step:
            step = steps[i]
            if step.state != 'COMPLETED':
                break

            start_time = dtparse(step.startdatetime)
            end_time = dtparse(step.enddatetime)
            elapsed = (end_time - start_time).total_seconds()
            times.append(elapsed)

            i += 1

        return times

    def _get_keyname(self, *args):
        """
        (override method)

        Returns the key name to use in the grading bucket (for the
        particular team and dataset).

            'team_id/infile/...'
        """

        return '%s/%s/%s' % (self.team_id, self._infile, '/'.join(args))
