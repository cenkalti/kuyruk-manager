# coding=utf8
from setuptools import setup

setup(
    name='Kuyruk-Manager',
    version="3.0.4",
    author=u'Cenk Altı',
    author_email='cenkalti@gmail.com',
    keywords='kuyruk manager',
    url='https://github.com/cenkalti/kuyruk-manager',
    packages=["kuyruk_manager"],
    include_package_data=True,
    install_requires=[
        'kuyruk>=9.2.2',
        'redis>=2.10',
        'Flask>=0.10',
        'waitress>=1.0.2',
    ],
    entry_points={
        'kuyruk.config': 'manager = kuyruk_manager.__init__:CONFIG',
        'kuyruk.commands': 'manager = kuyruk_manager:__init__.command'},
    description='Manage Kuyruk workers.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System :: Distributed Computing',
    ],
)
