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
RUN conda run -n openeo_odc_driver pip install odc-tools/apps/dc_tools

ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache

# RUN pip install --requirement /requirements.txt
RUN git clone https://github.com/interTwin-eu/openeo-processes-dask.git --recurse-submodules -b feature/load_stac_dev
# RUN git clone https://github.com/interTwin-eu/openeo-processes.git
# RUN mv openeo-processes openeo-processes-dask/specs/

RUN conda run -n openeo_odc_driver pip install openeo-processes-dask/.[implementations]

RUN git clone https://github.com/clausmichele/openeo-pg-parser-networkx.git -b rename_root_node
RUN conda run -n openeo_odc_driver pip install openeo-pg-parser-networkx/.

RUN conda run -n openeo_odc_driver pip install adlfs

RUN rm -rf /root/miniconda3/envs/openeo_odc_driver/lib/python3.11/site-packages/pydantic

RUN conda run -n openeo_odc_driver pip install pydantic==2.8.2


COPY . /openeo_odc_driver

# WORKDIR /

# ENTRYPOINT ["/tini", "--"]

ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/root/miniconda3/envs/openeo_odc_driver/lib/"

WORKDIR /openeo_odc_driver/openeo_odc_driver/

CMD ["conda", "run", "-n", "openeo_odc_driver", "--live-stream","gunicorn","-c","gunicorn.conf.py","odc_backend:app"]

