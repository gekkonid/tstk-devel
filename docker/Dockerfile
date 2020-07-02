FROM continuumio/miniconda3:latest

WORKDIR /usr/src/pyts2

# this is the most time consuming task.
# separating it out from the main install means that we can cache it and only bust 
# the cache when environment.yml is updated
COPY environment.yml .
RUN conda env update -n base -f environment.yml

RUN apt update && apt install -y --no-install-recommends libgl-dev libtiff-dev
COPY . .
RUN python setup.py install

COPY docker/entrypoint /bin/entrypoint

ENV EPHEMERAL="" \
    RESOURCES_OUTPUT="" \
    RESOURCES_BUNDLE="none" \
    DOWNSIZED_OUTPUT="" \
    DOWNSIZED_SIZE="720x" \
    DOWNSIZED_BUNDLE="none" \
    INFORMAT="jpg" \
    TELEGRAF_HOST="localhost" \
    TELEGRAF_METRIC="tstk_live_ingest" \
    TELEGRAF_PORT=8092

CMD ["/bin/entrypoint"]
