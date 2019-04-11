import os
import signal
import sys
from asyncio import get_event_loop, sleep
from typing import Any, Callable, Dict, List, Optional

from sanic import Sanic, request, response

from .manager import FunctionManager
from .modules.function import ArgDefinition, validate_arg
from .modules.task import TaskStoreSettings
from .utils.logger import get_logger


class FunctionServer:
    MAIN_PROCESS_ID = os.getpid()  # Don't rewrite.

    def __init__(
            self,
            shutdown_mode: str,
            port: int = 8888,
            timeout: int = 60*60*24,
            polling_interval: float = 1.0,
            debug: bool = False,
            register_sys_signals: bool = True,
            task_store_settings: TaskStoreSettings = TaskStoreSettings()):
        """Setup FunctionServer.

        Parameters
        ----------
        shutdown_mode
            An options to shutdown processes forked by FunctionServer.
            Acceptable Options are 'join' and 'terminate'.
            'join' is waiting until the processes finished.
            'terminate' is killing the processes immediately.
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

        self._funcname_endpoint_dict: Dict[str, str] = {}
        self._function_manager = FunctionManager(task_store_settings, debug)

        self._app = Sanic()

        self._port = port

        self._app.config.REQUEST_TIMEOUT = timeout
        self._app.config.RESPONSE_TIMEOUT = timeout

        self._polling_interval = polling_interval

        self._debug = debug

        self._shutdown_mode = shutdown_mode

        self._register_sys_signals = register_sys_signals

        self._logger = get_logger(self.__class__.__name__, debug)

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
                    signal.signal(signal.SIGINT, lambda _, __: self.exit_with_terminate())  # NOQA
                    self.exit_with_join()

                else:
                    raise ValueError

            signal.signal(signal.SIGINT, sig_handler)

        self._app.run(
            host='0.0.0.0',
            port=self._port,
            workers=1,
            register_sys_signals=False)

    def _construct_endpoints(self):
        """Splited to Unit Test with no server running."""
        # Functions
        @self._app.route('/api/list/data')
        async def get_api_list_data(request: request.Request):
            entrypoints = [self._funcname_endpoint_dict[name] for name in self._funcname_endpoint_dict]
            return response.json(entrypoints)

        @self._app.route('/api/list/text')
        async def get_api_list_text(request: request.Request):
            function_definitions = self._function_manager.definitions

            rows = []
            for name in function_definitions:
                elm = function_definitions[name]
                endpoint_name = self._funcname_endpoint_dict[name]

                rows.append(name)
                rows.append('  URL:')
                rows.append(f'    async api: /call/{endpoint_name}')
                rows.append(f'    block api: /call/blocking/{endpoint_name}')
                rows.append(f'  Max Concurrency: {elm.max_concurrency}')
                rows.append('  Description:')
                rows.append(f'        {elm.description}')
                if len(elm.arg_definitions) == 0:
                    rows.append('  No Args')
                else:
                    rows.append('  Args')
                    for arg in elm.arg_definitions:
                        rows.append(f'    {arg.name} {arg.type.name} {"Requiered" if arg.is_required else "NOT-Required"}')  # NOQA
                        if arg.description != '':
                            rows.append(f'      {arg.description}')
                rows.append('\n')

            return response.text('\n'.join(rows))

        # function
        @self._app.route('/api/function/definition/<func_name>')
        async def get_api_max_concurrency(request: request.Request, func_name: str):
            if func_name not in self._function_manager.definitions:
                return response.json({}, 404)
            return response.json(self._function_manager.definitions[func_name].to_dict())

        @self._app.route('/api/function/running_count/<func_name>')
        async def get_api_current_concurrency(request: request.Request, func_name: str):
            ret = self._function_manager.get_current_number_of_execution(func_name)
            if ret is None:
                return response.json({}, 404)
            return response.json(ret, 200)

        # Tasks
        @self._app.route('/task/info/<task_id>')
        async def get_task_info(request: request.Request, task_id: str):
            task_info = self._function_manager.get_task_info(task_id)
            if task_info is None:
                return response.json({}, 404)
            return response.json(task_info.to_dict())

        @self._app.route('/task/done/<task_id>')
        async def get_task_done(request: request.Request, task_id: str):
            task_info = self._function_manager.get_task_info(task_id)
            if task_info is None:
                return response.json({}, 404)
            return response.json(task_info.is_done())

        @self._app.route('/task/result/<task_id>')
        async def get_task_result(request: request.Request, task_id: str):
            task_info = self._function_manager.get_task_info(task_id)
            if task_info is None:
                return response.json({}, 404)
            return response.json(task_info.result)

        @self._app.route('/task/list/<func_name>')
        async def get_list_tasks(request: request.Request, func_name: str):
            tasks = self._function_manager.list_task_info(func_name)
            if tasks is None:
                return response.json({}, 404)
            return response.json([elm.to_dict() for elm in tasks])

        @self._app.post('/terminate/function/<func_name>')
        async def post_terminate_function(request: request.Request, func_name: str):
            self._function_manager.terminate_function(func_name)
            return response.json({}, 200)

        @self._app.post('/terminate/task/<task_id>')
        async def post_terminate_task(request: request.Request, task_id: str):
            self._function_manager.terminate_task(task_id)
            return response.json({}, 200)

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

        if len(data) < sum([elm.is_required for elm in arg_definitions]):
            raise ValueError('Parameter Missing')

        func_args = {}

        for arg_def in arg_definitions:
            if arg_def.name in data:
                raw = data[arg_def.name]
                validated = validate_arg(raw, arg_def.type)
                if not validated.is_ok:
                    raise ValueError(f'num_required_argsParameter is Invalid: {arg_def.name} {raw} {arg_def.type.name}')  # NOQA
                func_args[arg_def.name] = validated.value

            elif arg_def.is_required:
                raise ValueError(f'Parameter Missing: {arg_def.name}')

        return func_args

    def exit_with_terminate(self):
        """Kill the processes forked by FunctionServer."""
        if FunctionServer.MAIN_PROCESS_ID == os.getpid():
            self._function_manager.terminate_processes()
            get_event_loop().stop()
        else:
            sys.exit(0)

    def exit_with_join(self):
        """Wait all the processes finish."""
        if FunctionServer.MAIN_PROCESS_ID == os.getpid():
            self._function_manager.join_processes()
            get_event_loop().stop()

    def add_function(
            self,
            func: Callable,
            arg_definitions: List[ArgDefinition],
            max_concurrency: int = 0,
            description: str = '',
            endpoint_name: Optional[str] = None):
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
        endpoint_name
            REST API's URL name. (the default is None, which uses the function name.)

        Raises
        ------
        ValueError
            For max_concurrency.

        """
        if max_concurrency < 0:
            raise ValueError

        self._function_manager.add_function(
            func,
            arg_definitions,
            max_concurrency,
            description
        )

        if endpoint_name is None:
            endpoint_name = func.__name__

        self._funcname_endpoint_dict[func.__name__] = endpoint_name

        api_endpoint = f'/call/{endpoint_name}'
        api_blocking_endpoint = f'/call/blocking/{endpoint_name}'

        @self._app.post(api_endpoint)
        async def post_task_function(request: request.Request):
            self._logger.info(f'Task Requested : Endpoint {api_endpoint} : Payload {request.json}')  # NOQA

            try:
                func_args = self._generate_func_args(
                    arg_definitions,
                    request.json)
            except ValueError as e:
                self._logger.info(e)
                return response.json({'error': str(e)}, 500)

            result = self._function_manager.launch_function(func.__name__, func_args)
            return response.json(result.to_dict())

        @self._app.post(api_blocking_endpoint)
        async def post_task_blocking_function(request: request.Request):
            self._logger.info(f'Task Requested : Endpoint {api_blocking_endpoint} : Payload {request.json}')

            try:
                func_args = self._generate_func_args(
                    arg_definitions,
                    request.json)
            except ValueError as e:
                self._logger.info(e)
                return response.json({'error': str(e)}, 500)

            result = self._function_manager.launch_function(func.__name__, func_args)

            if not result.success:
                return response.json(result.to_dict())

            self._logger.info(f'Start Polling, func: {func.__name__}, task_id: {result.task_id}')

            task_info = None
            while True:
                task_info = self._function_manager.get_task_info(result.task_id)

                if task_info is None:
                    return response.json({'message': 'Something Unexpected.'}, 500)

                if task_info.is_done():
                    break

                await sleep(self._polling_interval)

            return response.json(task_info.result)

        self._logger.debug(f'Added: {api_endpoint}')
