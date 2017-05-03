# coding=utf8
from setuptools import setup

setup(
    name='Kuyruk-Manager',
    version="1.2.2",
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
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System :: Distributed Computing',
    ],
)
