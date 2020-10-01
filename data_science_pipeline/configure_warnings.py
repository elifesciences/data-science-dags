import logging
import warnings


warnings.filterwarnings(
    'ignore', 'Your application has authenticated using end user credentials',
    category=UserWarning
)

# suppressing: `Importing plotly failed. Interactive plots will not work.`
# see https://github.com/facebook/prophet/pull/1332
logging.getLogger('fbprophet.plot').setLevel(logging.CRITICAL)
