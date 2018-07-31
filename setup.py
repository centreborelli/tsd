from setuptools import setup, find_packages
import os

def readme():
    with open('README.md') as f:
        return f.read()


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="tsd",
    version="0.1",
    description='Automatic download of Sentinel, Landsat and Planet crops.',
    long_description=readme(),
    author='Carlo de Franchis (CMLA)'
           'With contributions from Enric Meinhardt-Llopis (ENS Cachan), Axel Davy (CMLA) and Tristan Dagobert (CMLA)',
    packages=find_packages(),
    install_requires=requirements,
    package_data={'': ['s2_mgrs_grid.txt']},
    include_package_data=True,
    python_requires='>=3.5',
    zip_safe=False
)
