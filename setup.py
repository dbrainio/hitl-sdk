from setuptools import setup, find_packages


install_requires = [
    'aiohttp==3.5.4',
    'dataclasses-json==0.3.5',
    'python-dateutil==2.8.0',
]

CONFIG = {
    'name': 'hitl-sdk',
    'url': '',
    'version': '0.2.6',
    'author': 'NilhEx',
    'install_requires': install_requires,
    'packages': find_packages(),
}

setup(**CONFIG)
