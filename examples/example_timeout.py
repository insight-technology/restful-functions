import os
import sys

# for develpment
try:
    import restful_functions  # NOQA
except Exception:
    print('using local version')
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
# ------------ #


import time

from restful_functions import FunctionServer


def long_process():
    time.sleep(100)


if __name__ == '__main__':
    # Make polling_timeout_process_interval a smaller value than defaults to confirm a terminated process due to timeout
    server = FunctionServer(polling_timeout_process_interval=1.0, debug=True)
    server.add_function(
        long_process,
        [],
        1,  # concurency
        'timeout test',
        None,
        10  # timeout
    )
    server.start()
