FROM andrejreznik/python-gdal:py3.10.0-gdal3.2.3

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    TINI_VERSION=v0.19.0

ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"

ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN apt-get update && \
    apt-get install -y \
      build-essential \
      git \
      wget \
      ffmpeg \
      libsm6 \
      libxext6 \
      libopengl0 \
      libegl1

# Install miniconda
RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh 
RUN conda --version

# RUN pip install --extra-index-url="https://packages.dea.ga.gov.au" \
#   odc-ui \
#   odc-stac \
#   odc-stats \
#   odc-algo \
#   odc-io \
#   odc-cloud[ASYNC] \
#   odc-dscache \
#   odc-index

COPY ./environment.yml /

RUN conda env create -f /environment.yml

RUN git clone https://github.com/clausmichele/odc-tools.git
RUN pip install odc-tools/apps/dc_tools

# RUN pip install --requirement /requirements.txt

ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache

COPY . /openeo_odc_driver

WORKDIR /

ENTRYPOINT ["/tini", "--"]

ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/root/miniconda3/envs/openeo_odc_driver/lib/"

WORKDIR /openeo_odc_driver/openeo_odc_driver/

ENTRYPOINT ["conda", "run", "-n", "openeo_odc_driver", "gunicorn","-c","gunicorn.conf.py","odc_backend:app"]

