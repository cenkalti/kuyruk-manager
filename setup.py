# coding=utf8
from setuptools import setup

setup(
    name='Kuyruk-Manager',
    version="1.0.0",
    author=u'Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='kuyruk manager',
    url='https://github.com/cenkalti/kuyruk-manager',
    py_modules=["kuyruk_manager"],
    install_requires=[
        'kuyruk>=3.0.0',
        'redis>=2.10',
        'Flask>=0.10',
        'rpyc>=3.3',
    ],
    entry_points={'kuyruk.config': 'manager = kuyruk_manager:CONFIG',
                  'kuyruk.commands': 'manager = kuyruk_manager:command'},
    description='Manage Kuyruk workers.',
    long_description=open('README.md').read(),
)
