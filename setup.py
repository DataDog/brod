#!/usr/bin/env python

import setuptools

install_requires = [
    "zc.zk==0.5.2",
]

setuptools.setup(
    name = 'brod',
    version = '0.4.3',
    license = 'MIT',
    description = """brod lets you produce messages to the Kafka distributed publish/subscribe messaging service. It started as a fork of pykafka (https://github.com/dsully/pykafka), but became a total rewrite as we needed to add many features.

It's named after Max Brod, Franz Kafka's friend and supporter.""",
    author = "Datadog, Inc.",
    author_email = "packages@datadoghq.com",
    url = 'https://github.com/datadog/brod',
    platforms = 'any',
    packages = ['brod'],
    zip_safe = True,
    verbose = False,
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'broderate = brod.util:broderate'
        ]
    },
)
