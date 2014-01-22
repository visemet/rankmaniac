####
## Configuration file.
####

# AWS credentials
AWS_ACCESS_KEY = 'YOUR-ACCESS-KEY'
AWS_SECRET_KEY = 'YOUR-SECRET-KEY'

# S3 buckets
S3_STUDENTS_BUCKET = 'cs144students'
S3_GRADING_BUCKET = 'cs144grading'

# Scoreboard client
SCORE_AUTH_TOKEN = 'YOUR-AUTH-TOKEN'
SCORE_UPLOAD_PATH = 'YOUR-UPLOAD-PATH'
SCORE_SESSION = 0

# Team names
long_names = {'foo': 'long foo team name',
              'bar': 'long bar team name',
              'baz': 'long baz team name'}
TEAMS = tuple(long_names.keys())

# Datasets
DATASETS = ('CaltechWeb', 'GNPn100p05')
