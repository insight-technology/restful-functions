from restful_functions import ArgDefinition, ArgType, FunctionServer


def test_api_list_with_no_functions():
    server = FunctionServer('terminate')

    server._construct_endpoints()

    req, res = server._app.test_client.get('/api/list/data')
    assert res.status == 200
    assert res.json == []

    req, res = server._app.test_client.get('/api/list/text')
    assert res.status == 200
    assert res.text == ''


def test_api_list_with_functions():
    def test1():
        pass

    def test2():
        pass

    server = FunctionServer('terminate')
    server.add_function(test1, [], 1)
    server.add_function(test2, [], 1)

    server._construct_endpoints()

    req, res = server._app.test_client.get('/api/list/data')
    assert res.status == 200
    assert res.json == ['test1', 'test2']

    req, res = server._app.test_client.get('/api/list/text')
    assert res.status == 200
    assert res.text != ''


def test_api_function_info():
    def test1():
        pass

    def test2():
        pass

    server = FunctionServer('terminate')
    server.add_function(test1, [], 1)
    server.add_function(test2, [ArgDefinition('x', ArgType.INTEGER, True, 'value x')], 2)

    server._construct_endpoints()

    req, res = server._app.test_client.get('/api/function/definition/test1')
    assert res.status == 200
    assert res.json['name'] == 'test1'
    assert len(res.json['arg_definitions']) == 0
    assert res.json['max_concurrency'] == 1

    req, res = server._app.test_client.get('/api/function/definition/test2')
    assert res.status == 200
    assert res.json['name'] == 'test2'
    assert len(res.json['arg_definitions']) == 1
    assert res.json['max_concurrency'] == 2

    req, res = server._app.test_client.get('/api/function/running_count/test1')
    assert res.status == 200
    assert res.json == 0

    req, res = server._app.test_client.get('/api/function/running_count/test2')
    assert res.status == 200
    assert res.json == 0

    req, res = server._app.test_client.get('/api/function/definition/test3')
    assert res.status == 404
    req, res = server._app.test_client.get('/api/function/running_count/test3')
    assert res.status == 404
