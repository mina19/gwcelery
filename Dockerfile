# build stage: build wheel + extract dependencies
FROM python:3.11 as build

ENV PIP_DEFAULT_TIMEOUT=100
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV POETRY_VERSION=1.8.1

WORKDIR /app

COPY . ./

RUN pip install --upgrade pip
RUN pip install \
    poetry==$POETRY_VERSION \
	poetry-dynamic-versioning

RUN poetry export -f requirements.txt -o requirements.txt
RUN poetry build -f wheel

# install stage: install gwcelery + dependencies
FROM python:3.11 as install

ENV PIP_DEFAULT_TIMEOUT=100
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

# install kerberos for ciecplib
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libkrb5-dev && \
	rm -rf /var/lib/apt /var/lib/dpkg /var/lib/cache /var/lib/log

WORKDIR /app

COPY --from=build /app/requirements.txt /app
COPY --from=build /app/dist/*.whl /app

# create virtualenv
RUN python -m venv /opt/venv

# ensure virtualenv python is preferred
ENV PATH="/opt/venv/bin:$PATH"

run pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install --no-deps *.whl

# final stage: installed app
FROM python:3.11-slim as app

COPY --from=install /opt/venv /opt/venv

# install kerberos for ciecplib
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libkrb5-dev && \
	rm -rf /var/lib/apt /var/lib/dpkg /var/lib/cache /var/lib/log

# create app user
RUN groupadd 1000 && \
    useradd --uid 1000 --gid 1000 -m gwcelery

USER gwcelery

WORKDIR /home/gwcelery

# ensure virtualenv python is preferred
ENV PATH="/opt/venv/bin:$PATH"

# disable OpenMP, MKL, and OpenBLAS threading by default
# it will be enabled selectively by processes that use it
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV OPENBLAS_NUM_THREADS=1

# web application configuration
ENV FLASK_RUN_PORT=5556
ENV FLASK_URL_PREFIX=/gwcelery
ENV FLOWER_PORT=5555
ENV FLOWER_URL_PREFIX=/flower

# set dev configuration by default
ENV CELERY_CONFIG_MODULE=gwcelery.conf.dev

ENTRYPOINT ["gwcelery"]
