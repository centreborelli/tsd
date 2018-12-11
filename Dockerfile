FROM carlodef/bionic-python3-gdal
MAINTAINER Carlo de Franchis <carlodef@gmail.com>
# RUN git clone https://github.com/cmla/tsd.git
WORKDIR /usr/src/app/tsd
COPY . .
RUN python3 setup.py install
WORKDIR /usr/src/app
