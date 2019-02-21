from setuptools import find_packages, setup

exec(open('restful_functions/_meta.py').read())

with open('requirements.txt', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

with open('test-requirements.txt', 'r') as f:
    test_requires = [x.strip() for x in f if x.strip()]

with open('dev-requirements.txt', 'r') as f:
    dev_requires = [x.strip() for x in f if x.strip()]

with open('README.rst', 'r') as f:
    readme = f.read()


setup(
    name='restful-functions',
    version=__version__,
    description="A Server Module to Build RESTful APIs for Python Functions",
    long_description=readme,
    url='https://github.com/insight-technology/restful-functions',
    license='MIT License',
    packages=find_packages(exclude=['examples', 'tests']),
    test_suite='tests',
    setup_requires=test_requires + dev_requires,
    tests_require=test_requires,
    install_requires=requires,
    extras_require={'dev': dev_requires, 'test': test_requires},
    python_requires='>=3.5',
    zip_safe=False,
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ),
)
