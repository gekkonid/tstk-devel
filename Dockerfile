FROM continuumio/miniconda3:latest

WORKDIR /usr/src/pyts2

COPY . .

RUN conda env update -n base -f environment.yml && python setup.py install

RUN apt update && apt install -y --no-install-recommends libgl-dev libtiff-dev
COPY run.sh .
COPY influx_ingest.py .

ENV SOURCE_DIR="" \
    DOWNSIZED_OUTPUT_DIR="" \
    BUNDLE_OUTPUT_DIR="" \
    IMAGE_FORMAT="jpg"

CMD ["./run.sh"]