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
          'singer-python==5.9.0',
          'squareup==5.3.0.20200528'
      ],
      extras_require={
          'dev': [
              'ipdb==0.11',
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
