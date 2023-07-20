from setuptools import setup, find_packages


install_requires = [
    'aiohttp==3.8.5',
    'dataclasses-json==0.3.5',
    'python-dateutil==2.8.0',
]

CONFIG = {
    'name': 'hitl-sdk',
    'url': '',
    'version': '0.4.2',
    'author': 'Dbrain',
    'install_requires': install_requires,
    'packages': find_packages(),
}

setup(**CONFIG)
