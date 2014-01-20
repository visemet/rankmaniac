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

from boto.exception import EmrResponseError

from config import ACCESS_KEY, SECRET_KEY
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

        Rankmaniac.__init__(self, team_id, ACCESS_KEY, SECRET_KEY,
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

        # Default modules for where to expect the pagerank step
        # and process step code
        pagerank_map = 'pagerank_map.py'
        pagerank_reduce = 'pagerank_reduce.py'
        process_map = 'process_map.py'
        process_reduce = 'process_reduce.py'

        if self._copy_keys(S3_STUDENTS_BUCKET, S3_GRADING_BUCKET):
            filename = '%s.cfg' % (self.team_id)
            if self._download_config(filename):
                # Read the configuration and override defaults
                config = ConfigParser.SafeConfigParser()
                config.read(filename)

                section = 'Rankmaniac'
                if config.has_section(section):
                    pagerank_map = config.get(section, 'pagerank_map')
                    pagerank_reduce = config.get(section, 'pagerank_reduce')
                    process_map = config.get(section, 'process_map')
                    process_reduce = config.get(section, 'process_reduce')

                for i in range(self._max_iter):
                    while True:
                        try:
                            self.do_iter(pagerank_map, pagerank_reduce,
                                         process_map, process_reduce)
                            break
                        except EmrResponseError:
                            sleep(10) # call Amazon APIs infrequently

                os.remove(filename)
                return True

        return False # signal set-up has failed

    def compute_job_time(self):
        """
        Returns the sum of the amount of time, in seconds, each step of
        the map-reduce job took.
        """

        return sum(self._compute_step_times())

    def compute_penalty(self):
        """
        Returns the amount of time, in seconds, to be added to the
        execution time as penalty for inaccuracy of results.
        """

        # TODO: implement
        return 0

    def _copy_keys(self, source_bucket_name, dest_bucket_name):
        """
        Copies the keys of a particular team from the source bucket to
        the destination bucket.
        """

        num_keys = 0

        source_bucket = self._s3_conn.get_bucket(source_bucket_name)
        dest_bucket = self._s3_conn.get_bucket(dest_bucket_name)

        # Clear out grading bucket/output contents for team
        keys = dest_bucket.list(prefix='%s/' % (self.team_id))
        dest_bucket.delete_keys(keys)

        keys = source_bucket.list(prefix='%s/' % (self.team_id))
        for key in keys:
            keyname = key.name
            suffix = keyname.split('/', 1)[1] # removes team identifier
            if '/' not in suffix and '$' not in suffix:
                key.copy(dest_bucket, keyname)
                num_keys += 1

        # Return whether anything was copied, i.e. previously submitted
        return num_keys > 0

    def _download_config(self, filename='rankmaniac.cfg'):
        """
        Downloads the configuration file for the given team. Returns
        `True` if the key was downloaded, and `False` otherwise.
        """

        bucket = self._s3_conn.get_bucket(self._s3_bucket)
        keyname = '%s/rankmaniac.cfg' % (self.team_id)
        key = bucket.get_key(keyname)

        if key is not None:
            key.get_contents_to_filename(filename)
            return True

        return False # signal key did not exist

    def _compute_step_times(self):
        """
        Returns the amount of time, in seconds, each step of the
        map-reduce job took as a list.
        """

        max_step = 0
        if self._last_process_step_iter_no >= 0:
            max_step = 2 * self._last_process_step_iter_no + 1

        times = []
        steps = self.describe().steps

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
