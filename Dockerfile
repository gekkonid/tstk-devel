FROM continuumio/miniconda3:latest
ADD . /tmp/pyts2
RUN cd /tmp/pyts2 && conda env update -n base -f environment.yml && python setup.py install && rm -rf /tmp/pts2
