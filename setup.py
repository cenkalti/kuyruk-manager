# coding=utf8
from setuptools import setup

setup(
    name='Kuyruk-Manager',
    version="1.1.0",
    author=u'Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='kuyruk manager',
    url='https://github.com/cenkalti/kuyruk-manager',
    packages=["kuyruk_manager"],
    include_package_data=True,
    install_requires=[
        'kuyruk>=6.0.0',
        'redis>=2.10',
        'Flask>=0.10',
        'rpyc>=3.3',
    ],
    entry_points={'kuyruk.config': 'manager = kuyruk_manager.__init__:CONFIG',
                  'kuyruk.commands': 'manager = kuyruk_manager:__init__.command'},
    description='Manage Kuyruk workers.',
    long_description=open('README.md').read(),
)
