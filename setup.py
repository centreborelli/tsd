from setuptools import setup, find_packages

requirements = ['area',
                'boto3',
                'bs4',
                'click',
                'future',
                'geojson',
                'google-auth',
                'lxml',
                'mgrs',
                'numpy>=1.12',
                'pandas',
                'planet',
                'pyproj',
                'python-dateutil',
                'rasterio[s3]>=1.0',
                'requests',
                'shapely',
                'tqdm',
                'utm']

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
