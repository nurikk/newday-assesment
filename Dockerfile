FROM jupyter/pyspark-notebook:python-3.11

WORKDIR /opt/application

RUN pip install --no-cache-dir --trusted-host pypi.python.org pipenv

COPY Pipfile .
COPY Pipfile.lock .

RUN pipenv install --system --deploy --dev