#!/usr/bin/env python

import setuptools

setuptools.setup(
  name = 'brod',
  version = '0.0.1',
  license = 'MIT',
  description = open('README.md').read(),
  author = "Datadog, Inc.",
  author_email = "packages@datadoghq.com",
  url = 'https://github.com/datadog/brod',
  platforms = 'any',
  packages = ['brod'],
  zip_safe = True,
  verbose = False,
)
