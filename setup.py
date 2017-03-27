#!/usr/bin/env python

from setuptools import setup, find_packages
from distutils.core import setup

setup(name='background_hang_reporter_job',
      version='0.1',
      description='An ETL job to identify and track problematic Firefox hangs.',
      author='Doug Thayer',
      author_email='dothayer@mozilla.com',
      url='https://github.com/squarewave/background-hang-reporter-job',
      packages=find_packages(exclude=['tests']),
      install_requires=[
        'eventlet',
      ]
)
