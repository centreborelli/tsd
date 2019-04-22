import os
from codecs import open
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

package = "tsd"

about = {}
with open(os.path.join(here, package, "__about__.py"), "r", "utf-8") as f:
    exec(f.read(), about)

def readme():
    with open(os.path.join(here, 'README.md'), 'r', 'utf-8') as f:
        return f.read()

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
                'sat-search>=0.2',
                'shapely',
                'tqdm',
                'utm',
                'sentinelhub']


setup(name=about["__title__"],
      version=about["__version__"],
      description=about["__description__"],
      long_description=readme(),
      long_description_content_type='text/markdown',
      url=about["__url__"],
      author=about["__author__"],
      author_email=about["__author_email__"],
      packages=[package],
      package_data={'': ['s2_mgrs_grid.txt']},
      include_package_data=True,
      install_requires=requirements,
      python_requires=">=3.5")
