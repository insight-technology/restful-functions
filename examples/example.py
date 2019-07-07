import os
import sys

# for develpment
try:
    import restful_functions  # NOQA
except Exception:
    print('using local version')
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
# ------------ #


from restful_functions import ArgDefinition, ArgType, FunctionServer


def addition(x: int, y: int):
    print(os.getpid(), os.getppid(), f'{x}+{y}={x+y}')
    return x+y


if __name__ == '__main__':
    server = FunctionServer('join', debug=True)
    server.add_function(
        addition,  # Function
        [
            ArgDefinition('x', ArgType.INTEGER, True, 'x'),
            ArgDefinition('y', ArgType.INTEGER, True, 'y'),
        ],  # Args
        1,  # Max Concurrency
        '足し算')  # Description
    server.start()
