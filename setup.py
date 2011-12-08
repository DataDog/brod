#!/usr/bin/env python

import setuptools

install_requires = [
    "zc.zk==0.2.0",
]

setuptools.setup(
    name = 'brod',
    version = '0.1.1',
    license = 'MIT',
    description = open('README.md').read(),
    author = "Datadog, Inc.",
    author_email = "packages@datadoghq.com",
    url = 'https://github.com/datadog/brod',
    platforms = 'any',
    packages = ['brod'],
    zip_safe = True,
    verbose = False,
    install_requires=install_requires
)
