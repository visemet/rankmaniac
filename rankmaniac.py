'''Simple Amazon Web Services Wrapper

Written for
    Rankmaniac 2013
    CS 144: Ideas behind the Web
    California Institute of Technology

Author
    Joe Wang
'''

import os
from time import localtime, strftime

import boto

# Amazon SDK for Elastic Map Reduce
from boto.emr.connection import EmrConnection
from boto.emr.step import StreamingStep

# Amazon SDK for S3
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class Rankmaniac:
    '''Rankmaniac Wrapper

    This class provides a simple wrapper around the Amazon Web Services SDK.
    It should provide all the functionality required in terms of MapReduce,
    so students don't need to worry about learning the EMR and S3 API.
    '''

    def __init__(self, team_id, access_key, secret_key,
                 bucket='cs144caltech'):
        '''Rankmaniac class constructor

        Creates a new instance of the Rankmaniac Wrapper for a specific
        team.

        Arguments:
            team_id         string      the team ID.
            access_key      string      AWS access key.
            secret_key      string      AWS secret key.
        '''


        boto.config.set('Boto', 'emr_region_name', 'us-west-2')
        boto.config.set('Boto', 'emr_region_endpoint',
                        'elasticmapreduce.us-west-2.amazonaws.com')

        self.s3_bucket = bucket

        self.team_id = team_id
        self.emr_conn = EmrConnection(access_key, secret_key)
        self.s3_conn = S3Connection(access_key, secret_key)
        self.job_id = None

        self._iter_no = 0
        self._infile = None
        self._last_outdir = None

    def __del__(self):

        if self.job_id:
            self.terminate_job()

    def set_input_file(self, filename):
        """
        TODO: document
        """

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
            self.emr_conn.add_jobflow_steps(self.job_id, steps)

        # Store `process_output` directory so it can be used in
        # subsequent iteration
        self._last_outdir = process_output
        self._iter_no += 1

    def _get_default_outdir(self, name):
        """
        TODO: document
        """

        # Return iter_no/name/ **with** the trailing slash
        return os.path.join(str(self._iter_no), name, '')

    def _submit_new_job(self, steps):
        """
        TODO: document
        """

        if self.job_id:
            raise Exception('There currently already exists a running job.')

        job_name = self._make_name()
        log_uri = self._get_s3_url() + 'job_logs'
        self.job_id = self.emr_conn.run_jobflow(name=job_name,
                                                steps=steps,
                                                num_instances=1,
                                                log_uri=log_uri,
                                                keep_alive=True)

    def submit_job(self, mapper, reducer, input, output, num_map=1,
                   num_reduce=1):
        '''Submit a new MapReduce job

        Submits a new MapReduce job with a single step. To add more steps,
        call add_step. To terminate this job, call terminate_job.

        Arguments:
            mapper          string      path to the mapper, relative to
                                        your data directory.
            reducer         string      path to the reducer, relative to
                                        your data directory.
            input           string      path to the input data, relative to
                                        your data directory. To specify a
                                        directory as input, ensure your path
                                        contains a trailing /.
            output          string      path to the desired output directory.
            num_map         int         number of map tasks for this job.
            num_reduce      int         number of reduce tasks for this job.
        '''

        if self.job_id:
            raise Exception('There currently already exists a running job.')

        job_name = self._make_name()
        step = self._make_step(mapper, reducer, input, output, num_map,
                               num_reduce)
        self.job_id = \
          self.emr_conn.run_jobflow(name=job_name,
                                    steps=[step],
                                    num_instances=1,
                                    log_uri=self._get_s3_url() + 'job_logs',
                                    keep_alive=True)

    def terminate_job(self):
        '''Terminate a running MapReduce job

        Stops the current running job.
        '''

        if not self.job_id:
            raise Exception('No job is running.')

        self.emr_conn.terminate_jobflow(self.job_id)
        self.job_id = None

    def get_job(self):
        '''Gets the running job details

        Returns:
            JobFlow object with relevant fields:
                state           string      the state of the job flow, either
                                            COMPLETED | FAILED | TERMINATED
                                            RUNNING | SHUTTING_DOWN | STARTING
                                            WAITING | BOOTSTRAPPING
                steps           list(Step)  a list of the step details in the
                                            workflow. A Step has the relevant
                                            fields:
                                                status              string
                                                startdatetime       string
                                                enddatetime         string

        Note: Amazon has an upper-limit on the frequency with which you can
              call this function; we have had success with calling it one
              every 10 seconds.
        '''

        if not self.job_id:
            raise Exception('No job is running.')

        return self.emr_conn.describe_jobflow(self.job_id)

    def add_step(self, mapper, reducer, input, output, num_map=1,
                 num_reduce=1):
        '''Add a step to an existing job

        Adds a new step to an already running job flow.

        Note: any given job flow can support up to 256 steps. To workaround
              this limitation, you can always choose to submit a new job
              once the current job completes.

        Arguments:
            mapper          string      path to the mapper, relative to
                                        your data directory.
            reducer         string      path to the reducer, relative to
                                        your data directory.
            input           string      path to the input data, relative to
                                        your data directory. To specify a
                                        directory as input, ensure your path
                                        contains a trailing /.
            output          string      path to the desired output directory.
        '''

        if not self.job_id:
            raise Exception('No job is running.')

        step = self._make_step(mapper, reducer, input, output, num_map,
                               num_reduce)
        self.emr_conn.add_jobflow_steps(self.job_id, [step])

    def upload(self, in_dir='data'):
        '''Upload local data to S3

        Uploads the files in the specified directory to S3, where it can be
        used by Elastic MapReduce.

        Note: this method only uploads files in the root of in_dir. It does
              NOT scan through subdirectories.

        Arguments:
            in_dir          string      optional, defaults to 'data'. Uses
                                        this directory as the base directory
                                        from which to upload.
        '''

        bucket = self.s3_conn.get_bucket(self.s3_bucket)
        keys = bucket.list(prefix='%s/' % self.team_id)
        bucket.delete_keys(map(lambda k: k.name, keys))

        to_upload = [
            (os.path.join(in_dir, file_name),
             os.path.join(self.team_id, file_name))
            for file_name in os.listdir(in_dir)
            if os.path.isfile(os.path.join(in_dir, file_name))]

        for l, r in to_upload:
            key = Key(bucket)
            key.key = r
            key.set_contents_from_filename(l)

    def download(self, out_dir='data'):
        '''Download S3 data to local directory

        Downloads S3 data to the specified directory.

        Note: this method DOES download the entire directory hierarchy as
              given by S3. It will create subdirectories as needed.

        Arguments:
            out_dir         string      optional, defaults to 'data'. Downloads
                                        files to this directory.
        '''

        bucket = self.s3_conn.get_bucket(self.s3_bucket)
        keys = bucket.list(prefix='%s/' % self.team_id)
        for key in keys:
            fp = os.path.join(out_dir, '/'.join(key.name.split('/')[1:]))
            fp_dir = os.path.dirname(fp)
            if os.path.exists(fp):
                os.remove(fp)
            elif not os.path.exists(fp_dir):
                os.makedirs(fp_dir)
            key.get_contents_to_filename(fp)

    def _make_name(self):

        return '%s-%s' % (self.team_id,
                          strftime('%m-%d-%Y %H:%M:%S', localtime()))


    def _make_step(self, mapper, reducer, input, output, nm=1, nr=1):

        job_name = self._make_name()
        team_s3 = self._get_s3_url()

        bucket = self.s3_conn.get_bucket(self.s3_bucket)
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

        return 's3n://%s/%s/' % (self.s3_bucket, self.team_id)
