"""
Client for the scoreboard.

Written for the Rankmaniac competition (2013-2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Shiyu Zhao (zjlszsy@gmail.com)
Edited by: Angela Gong (anjoola@anjoola.com)
Edited by: Max Hirschhorn (maxh@caltech.edu)
"""

import urllib
import urllib2

from config import SCORE_AUTH_TOKEN, SCORE_UPLOAD_PATH

def record_submission_time(team_id, runtime, penalty, session):
    """
    Records the runtime and penalty the specified team received for
    their submission.

    Arguments:
        team_id     <str>       the team identifier, which may be differ
                                slightly from the actual team name.

        runtime     <int>       the amount of time in seconds the
                                submission took to execute.

        penalty     <int>       the amount of time in seconds to
                                penalize the submission for inaccuracy.

        session     <int>       the session identifier, which is used
                                to indicate to the server when to add
                                the runtime and penalty for **multiple**
                                requests (i.e. datasets) that get run on
                                _same_ day.

    Special notes:
        The values of -1 and -2 (for runtime and penalty) are used to
        indicate whether a submission was invalid or nonexistent,
        respectively.
    """

    params = {}
    params['team_id'] = team_id
    params['duration'] = '%d %d' % (runtime, penalty)
    params['auth_token'] = SCORE_AUTH_TOKEN
    params['session'] = str(session)
    params = urllib.urlencode(params)

    # Make the request
    request = urllib2.Request(SCORE_UPLOAD_PATH, params)
    urllib2.urlopen(request)

def record_invalid_submission(team_id, session):
    """
    (convenience function)

    Records that the specified team had an invalid submission.
    """

    record_submission_time(team_id, -1, -1, session)

def record_no_submission(team_id, session):
    """
    (convenience function)

    Records that the specified team had no submission to run.
    """

    record_submission_time(team_id, -2, -2, session)
