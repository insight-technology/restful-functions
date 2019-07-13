restful-functions
=================

A Server Module to Build RESTful APIs for Python Fnctions.

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
        server = FunctionServer()
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

    $ curl localhost:8888/
    /function/list/data
    /function/list/text
    /function/definition/{function_name}
    /function/running-count/{function_name}
    /task/info/{task_id}
    /task/done/{task_id}
    /task/result/{task_id}
    /task/list/{function_name}
    /terminate/function/{function_name}
    /terminate/task/{task_id}

    $ curl localhost:8888/function/list/text
    addition
    URL:
        async api: /addition
        block api: /addition/keep-connection
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
        async api: /multi
        block api: /multi/keep-connection
    Max Concurrency: 2
    Description:
            Heavy Function
    No Args


    # Call Asynchronous
    # Obtain task_id
    $ curl -X POST -H "Content-Type: applicaiton/json" -d '{"x":3, "y":6}' http://localhost:8888/addition
    {"success": true, "message": "", "task_id": "c3a6a0ef-b19e-4e6f-bce3-8d0e5a9046aa"}

    # Obtain the result by task_id
    $ curl http://localhost:8888/task/info/3a6a0ef-b19e-4e6f-bce3-8d0e5a9046aa
    {"task_id": "c3a6a0ef-b19e-4e6f-bce3-8d0e5a9046aa", "function_name": "addition", "status": "DONE", "result": 9}

    $ curl http://localhost:8888/task/result/c3a6a0ef-b19e-4e6f-bce3-8d0e5a9046aa
    9

    # Call synchronous
    # Keeping the connection until the process ends.
    $ curl -X POST -H "Content-Type: applicaiton/json" -d '{"x":3, "y":6}' http://localhost:8888/addition/keep-connection
    9

    # Over Max Concurrency
    $ curl -X POST http://localhost:8888/multi
    {"success": true, "message": "", "task_id": "5bbbc1a0-74c2-4828-a843-fa2e2363e341"}

    $ curl -X POST http://localhost:8888/multi
    {"success": true, "message": "", "task_id": "7729af1f-c766-456e-a516-a75ab5f3a24c"}

    $ curl -X POST http://localhost:8888/multi
    {"success": false, "message": "Over Max Concurrency 2", "task_id": ""}


LICENSE
-------
MIT

TODO
----
[ ] Write Documents

[ ] Write CONTRIBUTING.md

[ ] Show Test Coverage

[ ] Deploy with CI Service
