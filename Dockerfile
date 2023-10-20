FROM andrejreznik/python-gdal:py3.10.0-gdal3.2.3

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    TINI_VERSION=v0.19.0

ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN apt-get update && \
    apt-get install -y \
      build-essential \
      git \
      wget \
      ffmpeg \
      libsm6 \ 
      libxext6 

COPY requirements.txt /conf/

RUN pip install --no-cache-dir --requirement /conf/requirements.txt
RUN pip install --extra-index-url="https://packages.dea.ga.gov.au" \
  odc-ui \
  odc-stac \
  odc-stats \
  odc-algo \
  odc-io \
  odc-cloud[ASYNC] \
  odc-dscache \
  odc-index

RUN git clone https://github.com/SARScripts/openeo_odc_driver.git -b dask_processes

WORKDIR /

ENTRYPOINT ["/tini", "--"]

WORKDIR /openeo_odc_driver/openeo_odc_driver/

CMD ["gunicorn","-c","gunicorn.conf.py","odc_backend:app"]
