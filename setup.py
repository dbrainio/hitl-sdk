from setuptools import setup, find_packages


install_requires = [
    'aiohttp',
]

CONFIG = {
    'name': 'hitl-sdk',
    'url': '',
    'version': '0.1.0',
    'author': 'NilhEx',
    'install_requires': install_requires,
    'packages': find_packages(),
}

setup(**CONFIG)
