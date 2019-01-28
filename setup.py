from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(name="tsd",
      version="0.2",
      description='Automatic download of Sentinel, Landsat and Planet crops.',
      author='Carlo de Franchis (CMLA)'
             'With contributions from Enric Meinhardt-Llopis (ENS Cachan), Axel Davy (CMLA) and Tristan Dagobert (CMLA)',
      packages=find_packages(),
      install_requires=requirements,
      package_data={'': ['s2_mgrs_grid.txt']},
      include_package_data=True,
      python_requires='>=3.5'
)
