[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Finsight-technology%2Frestful-functions.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Finsight-technology%2Frestful-functions?ref=badge_shield)

restful-functions
=================

A Server Module to Build RESTful APIs for Python Fnctions.

restful-functions uses `huge-success/sanic <https://github.com/huge-success/sanic>`_ that is FAST HTTP Server.

This framework forks a new process to run the specified function on each api request.

You can specify a maximum concurrency of the function. The api request is denied if the nubmer of running processes for the function is already reaches the concurrency limitation.

How to Use
----------

Example Code::

    import concurrent.futures
    import math

    from restful_functions import ArgDefinition, ArgType, FunctionServer


    def addition(x: int, y: int):
        """Simple Funciton."""
        return x+y


    def is_prime(n):
        if n % 2 == 0:
            return False

        sqrt_n = int(math.floor(math.sqrt(n)))
        for i in range(3, sqrt_n + 1, 2):
            if n % i == 0:
                return False
        return True


    def multi():
        """Heavy Function with MultiProcessing."""
        PRIMES = [
            112272535095293,
            112582705942171,
            112272535095293,
            115280095190773,
            115797848077099,
            1099726899285419]

        with concurrent.futures.ProcessPoolExecutor() as executor:
            for number, prime in zip(PRIMES, executor.map(is_prime, PRIMES)):
                print('%d is prime: %s' % (number, prime))

        return True


    if __name__ == '__main__':
        server = FunctionServer('join')
        server.add_function(
            addition,  # Function
            [
                ArgDefinition('x', ArgType.INTEGER, True, 'A value of X'),
                ArgDefinition('y', ArgType.INTEGER, True, 'A value of Y'),
            ],  # Args
            1,  # Max Concurrency
            'Simple Awesome Addition'  # Description
        )
        server.add_function(
            multi,
            [],
            2,
            'Heavy Function'
        )
        server.start()

Example Usage::

    $ curl localhost:8888/api/list/text
    addition
    URL:
        async api: /call/addition
        block api: /call/blocking/addition
    Max Concurrency: 1
    Description:
        Simple Awesome Addition
    Args
        x INTEGER Requiered
          A value of X
        y INTEGER Requiered
          A value of Y


    multi
    URL:
        async api: /call/multi
        block api: /call/blocking/multi
    Max Concurrency: 2
    Description:
        Heavy Function
    No Args


    # Call Asynchronous
    # Obtain task_id
    $ curl -X POST -H "Content-Type: applicaiton/json" -d '{"x":3, "y":6}' http://localhost:8888/call/addition
    {"success":true,"message":"","task_id":"3e4ad7cf-fc9a-41ca-8461-d9f344e9657d"}

    # Obtain the result by task_id
    $ curl http://localhost:8888/task/info/7a924f27-0f89-46a8-9ba7-76f0463b5ad4
    {"task_id":"3e4ad7cf-fc9a-41ca-8461-d9f344e9657d","function_name":"addition","status":"DONE","result":9}

    $ curl http://localhost:8888/task/result/7a924f27-0f89-46a8-9ba7-76f0463b5ad4
    9

    # Call synchronous
    # Keeping the connection until the process ends.
    $ curl -X POST -H "Content-Type: applicaiton/json" -d '{"x":3, "y":6}' http://localhost:8888/call/blocking/addition
    9

    # Over Max Concurrency
    $ curl -X POST http://localhost:8888/call/multi
    {"success":true,"message":"","task_id":"e7cd82dd-3cb3-4ada-9231-cb3522902757"}

    $ curl -X POST http://localhost:8888/call/multi
    {"success":true,"message":"","task_id":"d853197e-0179-4a64-9e00-fc10d3257995"}

    $ curl -X POST http://localhost:8888/call/multi
    {"success":false,"message":"Over Max Concurrency 2","task_id":""}



LICENSE
-------
MIT

TODO
----
[ ] Write Documents

[ ] Comments on Code

[ ] Write CONTRIBUTING.md

[ ] Test with Tox

[ ] Show Test Coverage

[ ] Deploy with CI Service


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Finsight-technology%2Frestful-functions.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Finsight-technology%2Frestful-functions?ref=badge_large)