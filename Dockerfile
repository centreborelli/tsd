FROM carlodef/bionic-python3-gdal
MAINTAINER Carlo de Franchis <carlodef@gmail.com>
RUN git clone https://github.com/cmla/tsd.git
RUN cd tsd && python3 setup.py install
WORKDIR /root
