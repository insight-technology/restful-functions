from restful_functions import ArgDefinition, ArgType, FunctionServer
from pytest_aiohttp import aiohttp_client
import pytest


@pytest.fixture
async def test_api_list_with_no_functions():
    server = FunctionServer(shutdown_mode='terminate')

    server._construct_endpoints()

    client = await aiohttp_client(server._app)

    res = await client.get('/function/list/data')
    assert res.status == 200
    assert await res.json() == []

    res = await client.get('/function/list/text')
    assert res.status == 200
    assert await res.text() == ''


@pytest.fixture
async def test_api_list_with_functions():
    def test1():
        pass

    def test2():
        pass

    server = FunctionServer(shutdown_mode='terminate')
    server.add_function(test1, [], 1)
    server.add_function(test2, [], 1)

    server._construct_endpoints()

    client = await aiohttp_client(server._app)

    res = await client.get('/api/list/data')
    assert res.status == 200
    assert await res.json() == ['test1', 'test2']

    res = await client.get('/api/list/text')
    assert res.status == 200
    assert await res.text() != ''


@pytest.fixture
async def test_api_list_function_with_different_endpoint():
    def function():
        pass

    server = FunctionServer(shutdown_mode='terminate')
    server.add_function(function, [], 1)
    server.add_function(function, [], 1, '', 'function_another_name')

    server._construct_endpoints()

    client = await aiohttp_client(server._app)

    res = await client.get('/api/list/data')
    assert res.status == 200
    assert await res.json() == ['function', 'function_another_name']

    res = await client.get('/api/list/text')
    assert res.status == 200
    assert await res.text() != ''


@pytest.fixture
async def test_api_function_info():
    def test1():
        pass

    def test2():
        pass

    server = FunctionServer(shutdown_mode='terminate')
    server.add_function(test1, [], 1)
    server.add_function(test2, [ArgDefinition('x', ArgType.INTEGER, True, 'value x')], 2)

    server._construct_endpoints()

    client = await aiohttp_client(server._app)

    res = client.get('/api/function/definition/test1')
    assert res.status == 200

    data = await res.json()
    assert data['name'] == 'test1'
    assert len(data['arg_definitions']) == 0
    assert data['max_concurrency'] == 1

    res = await client.get('/api/function/definition/test2')
    assert res.status == 200

    data = await res.json()
    assert data['name'] == 'test2'
    assert len(data['arg_definitions']) == 1
    assert data['max_concurrency'] == 2

    res = client.get('/api/function/running_count/test1')
    assert res.status == 200
    assert await res.json() == 0

    res = client.get('/api/function/running_count/test2')
    assert res.status == 200
    assert await res.json() == 0

    res = client.get('/api/function/definition/test3')
    assert res.status == 404

    res = client.get('/api/function/running_count/test3')
    assert res.status == 404
