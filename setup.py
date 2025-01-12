#!/usr/bin/env python

from setuptools import setup

setup(name='tap-square',
      version='2.2.0',
      description='Singer.io tap for extracting data from the Square API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_square'],
      install_requires=[
          'singer-python==5.10.0',
          'squareup==39.1.0.20241218',
          'backoff==1.8.0',
          'methodtools==0.4.2',
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint==2.5.3',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-square=tap_square:main
      ''',
      packages=['tap_square'],
      package_data = {
          'tap_square/schemas': [
              'items.json'
          ],
      },
      include_package_data=True,
)
