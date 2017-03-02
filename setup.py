#!/usr/bin/env python

from setuptools import setup

setup(name='tap-square',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Square API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_square'],
      install_requires=[
          'singer-python>=0.1.0',
          'requests==2.12.4',
      ],
      entry_points='''
          [console_scripts]
          tap-square=tap_square:main
      ''',
      packages=['tap_square'],
      package_data = {
          'tap_square/schemas': [
              'agents.json',
              'companies.json',
              'contacts.json',
              'conversations.json',
              'groups.json',
              'roles.json',
              'satisfaction_ratings.json',
              'tickets.json',
              'time_entries.json',
          ],
      },
      include_package_data=True,
)
