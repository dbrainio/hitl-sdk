from setuptools import setup, find_packages


install_requires = [
    'aiohttp==3.7.4',
    'dataclasses-json==0.3.5',
    'python-dateutil==2.8.0',
]

CONFIG = {
    'name': 'hitl-sdk',
    'url': '',
    'version': '0.3.9',
    'author': 'Dbrain',
    'install_requires': install_requires,
    'packages': find_packages(),
}

setup(**CONFIG)
