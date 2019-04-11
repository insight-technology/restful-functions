import os

from restful_functions import ArgDefinition, ArgType, FunctionServer


def addition(x: int, y: int):
    print(os.getpid(), os.getppid(), f'{x}+{y}={x+y}')
    return x+y


if __name__ == '__main__':
    server = FunctionServer('join')
    server.add_function(
        addition,  # Function
        [
            ArgDefinition('x', ArgType.INTEGER, True, 'x'),
            ArgDefinition('y', ArgType.INTEGER, True, 'y'),
        ],  # Args
        1,  # Max Concurrency
        '足し算')  # Description
    server.start()
