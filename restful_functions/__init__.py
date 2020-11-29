try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    # python <= 3.7
    import importlib_metadata  # type: ignore

__version__ = importlib_metadata.version(__name__)  # type: ignore

from .modules.function import ArgType, ArgDefinition
from .modules.task import TaskStoreSettings
from .server import FunctionServer

__all__ = [
    'ArgType',
    'ArgDefinition',
    'FunctionServer',
    'TaskStoreSettings',
    '__version__',
    ]
