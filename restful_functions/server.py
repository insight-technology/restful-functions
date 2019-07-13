import asyncio
import os
import signal
import sys
from typing import Any, Callable, Dict, List, Optional

from aiohttp import web

from .manager import FunctionManager
from .modules.function import ArgDefinition, validate_arg
from .modules.task import TaskStoreSettings
from .utils.logger import get_logger


class FunctionServer:
    MAIN_PROCESS_ID = os.getpid()  # Don't rewrite.

    def __init__(
            self,
            *,
            shutdown_mode: str = 'join',
            host: str = '0.0.0.0',
            port: int = 8888,
            timeout: int = 60*60*24,
            polling_interval: float = 1.0,
            debug: bool = False,
            register_sys_signals: bool = True,
            task_store_settings: TaskStoreSettings = TaskStoreSettings(),
            loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()):
        """Setup FunctionServer.

        Parameters
        ----------
        shutdown_mode
            An options to shutdown processes forked by FunctionServer.
            Acceptable Options are 'join' and 'terminate'.
            'join' is waiting until the processes finished.
            'terminate' is killing the processes immediately.
        host
            RESTful APIs host. (the default is '0.0.0.0')
        port
            RESTful APIs port. (the default is 8888)
        timeout
            Timeout of keeping the connection after a connection established.
            This is measured in Second.
            (the default is 60*60*24)
        polling_interval
            An interval of confirming processes runnning.
            FuncitonServer repeats checking a process status when you use blocking API.
            (the default is 1.0)
        debug
            Is Debug Mode or Not. (the default is False)
        register_sys_signals
            Registering restful-functions' Custom Signal Handler for SIG_TERM and SIG_INT. (the default is True)
        task_store_settings
            TaskStoreSettings. (the default is TaskStoreSettings(), which uses SQLite.)
        loop
            asyncio event loop.

        Raises
        ------
        ValueError
            For 'shutodown_mode', 'timeout', 'polling_interval'.

        """
        if shutdown_mode not in ['terminate', 'join']:
            raise ValueError('"shutdown_mode" should be "terminate" or "join"')

        if timeout <= 0:
            raise ValueError('"timeout" should be greater than 0')

        if polling_interval <= 0.0:
            raise ValueError('"polling_interval" should be greater than 0.0')

        self._function_manager = FunctionManager(task_store_settings, debug)

        self._app = web.Application(
            middlewares=(web.normalize_path_middleware(append_slash=False, remove_slash=True),)
        )
        self._runner = web.AppRunner(self._app, handle_signals=False)

        self._host = host
        self._port = port

        self._polling_interval = polling_interval

        self._debug = debug

        self._shutdown_mode = shutdown_mode

        self._register_sys_signals = register_sys_signals

        self._loop = loop

        self._logger = get_logger(self.__class__.__name__, debug=debug)

    def start(self):
        """Start FunctionServer Process."""
        self._construct_endpoints()

        if self._register_sys_signals:
            def sig_handler(signum, frame):
                if self._shutdown_mode == 'terminate':
                    self.exit_with_terminate()

                elif self._shutdown_mode == 'join':
                    if FunctionServer.MAIN_PROCESS_ID == os.getpid():
                        print('Joining Processes now.')
                        print('Press Ctrl+C again to force exit.')

                    signal.signal(signal.SIGINT, lambda _, __: self.exit_with_terminate())
                    self.exit_with_join()

                else:
                    raise ValueError

            signal.signal(signal.SIGINT, sig_handler)

        async def server_coro():
            await self._runner.setup()

            site = web.TCPSite(self._runner, host=self._host, port=self._port)

            await site.start()

            self._logger.info(f'Start Server {self._host}:{self._port}')

            while True:
                await asyncio.sleep(3600)

        self._loop.run_until_complete(server_coro())

    def _construct_endpoints(self):
        """Splited to Unit Test with no server running."""
        # Functions
        async def get_function_list_data(request: web.Request):
            entrypoints = [elm.to_dict() for elm in self._function_manager.definitions.values()]
            return web.json_response(entrypoints)

        async def get_function_list_text(request: web.Request):
            rows = []
            for definition in self._function_manager.definitions.values():
                rows.append(definition.function_name)
                rows.append('  URL:')
                rows.append(f'    async api: /{definition.function_name}')
                rows.append(f'    block api: /{definition.function_name}/keep-connection')
                rows.append(f'  Max Concurrency: {definition.max_concurrency}')
                rows.append('  Description:')
                rows.append(f'        {definition.description}')
                if len(definition.arg_definitions) == 0:
                    rows.append('  No Args')
                else:
                    rows.append('  Args')
                    for arg in definition.arg_definitions:
                        rows.append(f'    {arg.name} {arg.type.name} {"Requiered" if arg.is_required else "NOT-Required"}')
                        if arg.description != '':
                            rows.append(f'      {arg.description}')
                rows.append('\n')

            return web.Response(text='\n'.join(rows))

        # function
        async def get_function_definition(request: web.Request):
            function_name = request.match_info['function_name']

            if function_name not in self._function_manager.definitions:
                raise web.HTTPNotFound()

            return web.json_response(self._function_manager.definitions[function_name].to_dict())

        async def get_function_running_count(request: web.Request):
            function_name = request.match_info['function_name']

            ret = self._function_manager.get_current_number_of_execution(function_name)
            if ret is None:
                raise web.HTTPNotFound()

            return web.json_response(ret)

        # Tasks
        async def get_task_info(request: web.Request):
            if 'task_id' not in request.match_info:
                raise web.HTTPBadRequest()

            task_id = request.match_info['task_id']

            task_info = self._function_manager.get_task_info(task_id)
            if task_info is None:
                raise web.HTTPNotFound()

            return web.json_response(task_info.to_dict())

        async def get_task_done(request: web.Request):
            if 'task_id' not in request.match_info:
                raise web.HTTPBadRequest()

            task_id = request.match_info['task_id']

            task_info = self._function_manager.get_task_info(task_id)
            if task_info is None:
                raise web.HTTPNotFound()

            return web.json_response(task_info.is_done())

        async def get_task_result(request: web.Request):
            if 'task_id' not in request.match_info:
                raise web.HTTPBadRequest()

            task_id = request.match_info['task_id']

            task_info = self._function_manager.get_task_info(task_id)
            if task_info is None:
                raise web.HTTPNotFound()
            return web.json_response(task_info.result)

        async def get_task_list(request: web.Request):
            if 'function_name' not in request.match_info:
                raise web.HTTPBadRequest()

            function_name = request.match_info['function_name']

            tasks = self._function_manager.list_task_info(function_name)
            if tasks is None:
                raise web.HTTPNotFound()

            return web.json_response([elm.to_dict() for elm in tasks])

        # Termination
        async def post_terminate_function(request: web.Request):
            if 'function_name' not in request.match_info:
                raise web.HTTPBadRequest()

            function_name = request.match_info['function_name']

            self._function_manager.terminate_function(function_name)
            return web.json_response({})

        async def post_terminate_task(request: web.Request, task_id: str):
            if 'task_id' not in request.match_info:
                raise web.HTTPBadRequest()

            task_id = request.match_info['task_id']
            self._function_manager.terminate_task(task_id)

            return web.json_response({})

        api_list = [
            web.get('/function/list/data', get_function_list_data),
            web.get('/function/list/text', get_function_list_text),
            web.get(r'/function/definition/{function_name}', get_function_definition),
            web.get(r'/function/running-count/{function_name}', get_function_running_count),
            web.get(r'/task/info/{task_id}', get_task_info),
            web.get(r'/task/done/{task_id}', get_task_done),
            web.get(r'/task/result/{task_id}', get_task_result),
            web.get(r'/task/list/{function_name}', get_task_list),
            web.post(r'/terminate/function/{function_name}', post_terminate_function),
            web.post(r'/terminate/task/{task_id}', post_terminate_task),
        ]

        async def index(request: web.Request):
            return web.Response(text='\n'.join([elm.path for elm in api_list])+'\n')

        self._app.add_routes([*api_list, web.get('/', index)])

    def _generate_func_args(
            self,
            arg_definitions: List[ArgDefinition],
            data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """.

        Parameters
        ----------
        arg_definitions
            Definitions of Arguments.
        data
            Parameters for the target function Passed by API.

        Raises
        ------
        ValueError
            An Exception is raises if 'data' is invalid for 'arg_definitions'.

        Returns
        -------
        Dict[str, Any]
            Validated and formated data.

        """
        if data is None:
            data = {}

        func_args = {}

        for arg_def in arg_definitions:
            if arg_def.name in data:
                raw = data[arg_def.name]
                validated = validate_arg(raw, arg_def.type)
                if not validated.is_ok:
                    raise ValueError(f'Parameter is Invalid: {arg_def.name} {raw} {arg_def.type.name}')
                func_args[arg_def.name] = validated.value

            elif arg_def.is_required:
                raise ValueError(f'Parameter Missing: {arg_def.name}')

        return func_args

    def _exit_event_loop(self):
        self._loop.call_soon(lambda: asyncio.ensure_future(self._runner.cleanup()))
        self._loop.stop()

    def exit_with_terminate(self):
        """Kill the processes forked by FunctionServer."""
        if FunctionServer.MAIN_PROCESS_ID == os.getpid():
            self._function_manager.terminate_processes()
            self._exit_event_loop()

    def exit_with_join(self):
        """Wait all the processes finish."""
        if FunctionServer.MAIN_PROCESS_ID == os.getpid():
            self._function_manager.join_processes()
            self._exit_event_loop()
            sys.exit(0)

    def add_function(
            self,
            func: Callable,
            arg_definitions: List[ArgDefinition],
            max_concurrency: int = 0,
            description: str = '',
            function_name: str = ''):
        """Add Job to FunctionServer.

        Parameters
        ----------
        func
            A Python Function.
        arg_definitions
            Definitions of Arguments.
        max_concurrency
            A Limitation for number of parallel execution.
            (the default is 0, which tells there is No Limitation.)
        description
            A Description for The Function. (the default is '', which [default_description])
        function_name
            Function Name. It is not necessary to same with func.__name__.
            This values is used as REST API's endpoint.
            (the default is None, which uses the func.__name__.)

        Raises
        ------
        ValueError
            For max_concurrency.

        """
        if max_concurrency < 0:
            raise ValueError

        if function_name is not None:
            function_name = function_name.strip('/')

        if function_name is None or function_name == '':
            function_name = func.__name__

        self._function_manager.add_function(
            func,
            function_name,
            arg_definitions,
            max_concurrency,
            description
        )

        api_async_endpoint = f'/{function_name}'
        api_sync_endpoint = f'/{function_name}/keep-connection'

        async def post_async_function(request: web.Request):
            try:
                data = await request.json()
            except Exception:
                data = {}

            self._logger.info(f'Task Requested : Endpoint {api_async_endpoint} : Payload {data}')

            try:
                func_args = self._generate_func_args(arg_definitions, data)
            except ValueError as e:
                self._logger.info(e)
                return web.json_response({'error': str(e)}, status=400)

            result = self._function_manager.launch_function(function_name, func_args)
            return web.json_response(result.to_dict())

        async def post_sync_function(request: web.Request):
            try:
                data = await request.json()
            except Exception:
                data = {}

            self._logger.info(f'Task Requested : Endpoint {api_sync_endpoint} : Payload {data}')

            try:
                func_args = self._generate_func_args(arg_definitions, data)
            except ValueError as e:
                self._logger.info(e)
                return web.json_response({'error': str(e)}, status=400)

            result = self._function_manager.launch_function(function_name, func_args)

            if not result.success:
                return web.json_response(result.to_dict())

            self._logger.info(f'Start Polling, func: {function_name}, task_id: {result.task_id}')

            task_info = None
            while True:
                task_info = self._function_manager.get_task_info(result.task_id)

                if task_info is None:
                    return web.json_response({'message': 'Something Unexpected.'}, status=500)

                if task_info.is_done():
                    break

                await asyncio.sleep(self._polling_interval)

            return web.json_response(task_info.result)

        self._app.add_routes([
            web.post(api_async_endpoint, post_async_function),
            web.post(api_sync_endpoint, post_sync_function),
        ])

        self._logger.debug(f'Added: {api_async_endpoint}')
