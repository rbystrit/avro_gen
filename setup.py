"""
A setuptools-based setup module.

See:
https://github.com/rbystrit/avro_gen
"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='avro-gen',
    version='0.1.0b1',

    description='Avro record class and specific record reader generator',
    long_description=long_description,
    url='https://github.com/rbystrit/avro_gen',
    author='Roman Bystritskiy',
    author_email='rbystrit@gmail.com',
    license='License :: OSI Approved :: Apache Software License',

    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.7',
    ],

    keywords='avro class generator',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['avro >= 1.8.1'],
)
