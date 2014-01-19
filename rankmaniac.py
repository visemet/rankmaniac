"""
Simple wrapper for boto library to connect with AWS.

Written for the Rankmaniac competition (2013-2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Joe Wang (me@joewang.net)
Edited by: Max Hirschhorn (maxh@caltech.edu)
"""

import os
from time import localtime, strftime

# Amazon SDK for EC2
from boto.ec2.regioninfo import RegionInfo

# Amazon SDK for Elastic Map Reduce
from boto.emr.connection import EmrConnection
from boto.emr.step import StreamingStep

# Amazon SDK for S3
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class RankmaniacError(Exception):
    """General (catch-all) class for exceptions in this module."""
    pass

class Rankmaniac:
    """
    (wrapper class)

    This class presents a simple wrapper around the AWS SDK. It strives
    to provide all the functionality required to run map-reduce
    (Hadoop) on Amazon. This way the students do not need to worry about
    learning the API for Amazon S3 and EMR, and instead can focus on
    computing pagerank quickly!
    """

    DefaultRegionName = 'us-west-2'
    DefaultRegionEndpoint = 'elasticmapreduce.us-west-2.amazonaws.com'

    def __init__(self, team_id, access_key, secret_key,
                 bucket='cs144caltech'):
        """
        (constructor)

        Creates a new instance of the Rankmaniac class for a specific
        team using the provided credentials.

        Arguments:
            team_id       <str>     the team identifier, which may be
                                    differ slightly from the actual team
                                    name.

            access_key    <str>     the AWS access key identifier.
            secret_key    <str>     the AWS secret acess key.

        Keyword arguments:
            bucket        <str>     the S3 bucket name.
        """

        region = RegionInfo(None, self.DefaultRegionName,
                            self.DefaultRegionEndpoint)

        self._s3_bucket = bucket
        self._s3_conn = S3Connection(access_key, secret_key)
        self._emr_conn = EmrConnection(access_key, secret_key, region=region)

        self.team_id = team_id
        self.job_id = None

        self._reset()

    def _reset(self):
        """
        Resets the internal state of the job and submission.
        """

        self._iter_no = 0
        self._infile = None
        self._last_outdir = None

    def __del__(self):
        """
        (destructor)

        Terminates the map-reduce job if any, and closes the connections
        to Amazon S3 and EMR.
        """

        if self.job_id:
            self.terminate()

        self._s3_conn.close()
        self._emr_conn.close()

    def __enter__(self):
        """
        Used for `with` syntax. Simply returns this instance since the
        set-up has all been done in the constructor.
        """

        return self

    def __exit__(self, type, value, traceback):
        """
        Refer to __del__().
        """

        self.__del__()
        return False # do not swallow any exceptions

    def upload(self, indir='data'):
        """
        Uploads the local data to Amazon S3 under the configured bucket
        and key prefix (the team identifier). This way the code can be
        accessed by Amazon EMR to compute pagerank.

        Keyword arguments:
            indir       <str>       the base directory from which to
                                    upload contents.

        Special notes:
            This method only uploads **files** in the specified
            directory. It does not scan through subdirectories.

            WARNING! This method removes all previous (or ongoing)
            submission results, so it is unsafe to call while a job is
            already running (and possibly started elsewhere).
        """

        if self.job_id is not None:
            raise RankmaniacError('A job is already running.')

        bucket = self._s3_conn.get_bucket(self._s3_bucket)

        # Clear out current bucket contents for team
        keys = bucket.list(prefix='%s/' % (self.team_id))
        bucket.delete_keys(keys)

        for filename in os.listdir(indir):
            relpath = os.path.join(indir, filename)
            if os.path.isfile(relpath):
                keyname = '%s/%s' % (self.team_id, filename)
                key = bucket.new_key(keyname)
                key.set_contents_from_filename(relpath)

    def set_infile(self, filename):
        """
        Sets the data file to use for the first iteration of the
        pagerank step in the map-reduce job.
        """

        if self.job_id is not None:
            raise RankmaniacError('A job is already running.')

        self._infile = filename

    def do_iter(self, pagerank_mapper, pagerank_reducer,
                process_mapper, process_reducer,
                pagerank_output=None, process_output=None,
                num_pagerank_mappers=1, num_pagerank_reducers=1):
        """
        Adds a pagerank step and a process step to the current job.
        """

        num_process_mappers = 1
        num_process_reducers = 1

        if self._iter_no == 0:
            pagerank_input = self._infile
        elif self._iter_no > 0:
            pagerank_input = self._last_outdir

        if pagerank_output is None:
            pagerank_output = self._get_default_outdir('pagerank')

        # Output from the pagerank step becomes input to process step
        process_input = pagerank_output

        if process_output is None:
            process_output = self._get_default_outdir('process')

        pagerank_step = self._make_step(pagerank_mapper, pagerank_reducer,
                                        pagerank_input, pagerank_output,
                                        num_pagerank_mappers,
                                        num_pagerank_reducers)

        process_step = self._make_step(process_mapper, process_reducer,
                                       process_input, process_output,
                                       num_process_mappers,
                                       num_process_reducers)

        steps = [pagerank_step, process_step]
        if self.job_id is None:
            self._submit_new_job(steps)
        else:
            self._emr_conn.add_jobflow_steps(self.job_id, steps)

        # Store `process_output` directory so it can be used in
        # subsequent iteration
        self._last_outdir = process_output
        self._iter_no += 1

    def is_done(self):
        """
        Gets the first part of the output file and checks whether it
        startswith 'FinalRank'.

        Special notes:
            WARNING! The usage of this method in your code requires that
            that you used the default output directories in all calls
            to do_iter().
        """

        iter_no = self._get_last_process_step_iter_no()
        if iter_no < 0:
            return False

        outdir = self._get_default_outdir('process', iter_no=iter_no)
        keyname = '%s/%s/%s' % (self.team_id, outdir, 'part-00000')

        bucket = self._s3_conn.get_bucket(self._s3_bucket)
        key = Key(bucket=bucket, name=keyname)
        contents = key.next()

        return contents.startswith('FinalRank')

    def terminate(self):
        """
        Terminates a running map-reduce job.
        """

        if not self.job_id:
            raise RankmaniacError('No job is running.')

        self._emr_conn.terminate_jobflow(self.job_id)
        self.job_id = None

        self._reset()

    def download(self, outdir='results'):
        """
        Downloads the results from Amazon S3 to the local directory.

        Keyword arguments:
            outdir      <str>       the base directory to which to
                                    download contents.

        Special notes:
            This method downloads all keys (files) from the configured
            bucket for this particular team. It creates subdirectories
            as needed.
        """

        bucket = self._s3_conn.get_bucket(self._s3_bucket)
        keys = bucket.list(prefix='%s/' % (self.team_id))
        for key in keys:
            suffix = key.name.split('/')[1:] # removes team identifier
            filename = os.path.join(outdir, *suffix)
            dirname = os.path.dirname(filename)

            if not os.path.exists(dirname):
                os.makedirs(dirname)

            key.get_contents_to_filename(filename)

    def describe(self):
        """
        Gets the current map-reduce job details.

        Returns a boto.emr.emrobject.JobFlow object.

        Special notes:
            The JobFlow object has the following relevant fields.
                state       <str>           the state of the job flow,
                                            either COMPLETED
                                                 | FAILED
                                                 | TERMINATED
                                                 | RUNNING
                                                 | SHUTTING_DOWN
                                                 | STARTING
                                                 | WAITING

                steps       <list(boto.emr.emrobject.Step)>
                            a list of the step details in the workflow.

            The Step object has the following relevant fields.
                state               <str>       the state of the step.

                startdatetime       <str>       the start time of the
                                                job.

                enddatetime         <str>       the end time of the job.

            WARNING! Amazon has an upper-limit on the frequency with
            which you can call this method; we have had success with
            calling it at most once every 10 seconds.
        """

        if not self.job_id:
            raise Exception('No job is running.')

        return self._emr_conn.describe_jobflow(self.job_id)

    def _get_last_process_step_iter_no(self):
        """
        Returns the most recently process-step of the job flow that has
        been completed.
        """

        steps = self.describe().steps
        i = 1

        while i < len(steps):
            step = steps[i]
            if step.state != 'COMPLETED':
                break

            i += 2

        return i / 2 - 1

    def _get_default_outdir(self, name, iter_no=None):
        """
        TODO: document
        """

        if iter_no is None:
            iter_no = self._iter_no
        iter_no = str(iter_no)

        # Return iter_no/name/ **with** the trailing slash
        return os.path.join(iter_no, name, '')

    def _submit_new_job(self, steps):
        """
        TODO: document
        """

        if self.job_id:
            raise Exception('There currently already exists a running job.')

        job_name = self._make_name()
        log_uri = self._get_s3_url() + 'job_logs'
        self.job_id = self._emr_conn.run_jobflow(name=job_name,
                                                 steps=steps,
                                                 num_instances=1,
                                                 log_uri=log_uri,
                                                 keep_alive=True)

    def _make_name(self):

        return '%s-%s' % (self.team_id,
                          strftime('%m-%d-%Y %H:%M:%S', localtime()))


    def _make_step(self, mapper, reducer, input, output, nm=1, nr=1):

        job_name = self._make_name()
        team_s3 = self._get_s3_url()

        bucket = self._s3_conn.get_bucket(self._s3_bucket)
        keys = bucket.list(prefix='%s/%s' % (self.team_id, output))
        bucket.delete_keys(map(lambda k: k.name, keys))

        return \
            StreamingStep(name=job_name,
                          step_args=
                              ['-jobconf', 'mapred.map.tasks=%d' % nm,
                               '-jobconf', 'mapred.reduce.tasks=%d' % nr],
                          mapper=team_s3 + mapper,
                          reducer=team_s3 + reducer,
                          input=team_s3 + input,
                          output=team_s3 + output)

    def _get_s3_url(self):

        return 's3n://%s/%s/' % (self._s3_bucket, self.team_id)
