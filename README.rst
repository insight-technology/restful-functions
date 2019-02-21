restful-functions
=================

A Server Module to Build RESTful APIs for Python Fnctions. 

How to Use
----------

Example Code::

    import os

    from restful_functions import ArgDefinition, ArgType, FunctionServer


    def addition(x: int, y: int):
        return x+y


    if __name__ == '__main__':
        server = FunctionServer('join')
        server.add_job(
            addition,  # Function
            [
                ArgDefinition('x', ArgType.INTEGER, True, 'x'),
                ArgDefinition('y', ArgType.INTEGER, True, 'y'),
            ],  # Args
            1,  # Max Concurrency
            'Simple Awesome Addition')  # Description
        server.start()

Example Usage::

    $ curl http://localhost:8888/api/list/text
    addition
    URL:
        async api: /call/addition
        block api: /call/blocking/addition
    Max Concurrency: 1
    Description:
        Simple Awesome Addition
    Args
        x INTEGER Requiered
        x
        y INTEGER Requiered
        y

    # Call Asynchronous
    # Obtain task_id
    $ curl -X POST -H "Content-Type: applicaiton/json" -d '{"x":3, "y":6}' http://localhost:8888/call/addition
    {"successed":true,"message":"","task_id":"3e4ad7cf-fc9a-41ca-8461-d9f344e9657d"}

    # Obtain a result by task_id
    $ curl http://localhost:8888/task/status/7a924f27-0f89-46a8-9ba7-76f0463b5ad4
    {"status":"DONE","result":9}

    # Call synchronous
    # Keeping the connection until the process ends.
    $ curl -X POST -H "Content-Type: applicaiton/json" -d '{"x":3, "y":6}' http://localhost:8888/call/blocking/addition
    9


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
