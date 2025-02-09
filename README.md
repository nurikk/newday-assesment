# Newday Spark Job Assessment Project

```
Newday Data Engineer Interview
Homework
Introduction
This homework is intended to understand your capability in Spark and Scala/Python. The problem is posed quite loosely to allow you to solve it in the way you are most comfortable. We will be looking at the approach used, the code style, structure, and quality as well as testing.
Data
Download the movie lens open data set (ml-1m.zip) from
http://files.grouplens.org/datasets/movielens/ml-1m.zip
Brief
The job needs to do the following:
1. Read in movies.dat and ratings.dat to spark dataframes.
2. Creates a new dataframe, which contains the movies data and 3 new columns max, min and
average rating for that movie from the ratings data.
3. Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies
based on their rating.
4. Write out the original and new dataframes in an efficient format of your choice.
You should also include the command to build and run your code with spark-submit.
```
# Prerequisites
 - docker
 - make


## Run

```shell
make startdevenv # start test docker container
make unittest # run unittests in test container
make test-submit # run spark-submit with job in test container
```

Complete list of commands can be found using `make help` command

spark-submit command can be found in [submit.sh](./submit.sh)
to pass arguments to job use `--job-arg` parameter:
- `--job-arg output-format=csv` - use csv as output format
- `--job-arg destination=s3://my-bucket/here/` - store output datasets in s3
Job default parameters can be found in [main.py#L14](https://github.com/nurikk/newday-assesment/blob/d8a56aaa012e88b3ac8ccd2dec0f0fcf07a33522/jobs/newday/main.py#L14)
- `dataset-url=http://files.grouplens.org/datasets/movielens/ml-1m.zip`
- `destination=/tmp/`
- `output-format=parquet`

## CI
Repositoery contains github action to run unit tests and test-sumbit jobs on every push to main branch. Resulting artifcats (in csv and parquet formats) are persisted using githiub action and can be downloaded/inspected after every build
example run: https://github.com/nurikk/newday-assesment/actions/runs/7283349722

## Meta
I'm not a spark developer and don't write spark jobs on a daily basis,
to get most production ready solution it's better to rely on exising boilerplates. 
The main criteria to choose:
- Boilerplate should be easy to reuse
- Should use docker to isolate test environment from developer machine, freeing from installing apache spark locally

Quick googling on "pyspark job boilerplate" gives number of projects/samples to follow.

https://github.com/ekampf/PySpark-Boilerplate
- :arrow_down: quite old
- :arrow_down: manipulates with sys.path.insert
- :arrow_down: manually packages zip with dependencies
- :arrow_down: quite basic test samples

https://medium.com/@shangmin.j.jiang/pyspark-job-template-with-pex-d7de7236707e
- :arrow_up: dockerized solution
- :arrow_up: jobs as packages
- :arrow_up:uses PEX format
- :arrow_up: decent test samples
- :arrow_down: manual dependencies packaging, even though PEX

https://github.com/pratikbarjatya/pyspark-boilerplate-template
- :arrow_down:  quite similar to ekampf boilerplate

https://github.com/rodalbuyeh/pyspark-k8s-boilerplate
- :arrow_up:  k8s
- :arrow_up:  docker
- :arrow_up:  build with developer workflow in mind    
- :arrow_up:  quite interesting approach, running spark on k8s rather that dedidcated spark cluster
- :arrow_down: GCP only example, but can be refactored for AWS EKS, Azure AKS
- :arrow_down: a bit overkill for a interview assessment

It's made decision to use example from @shangmin.j.jiang with some changes:
- using jupyter/pyspark-notebook base dev docker image, it supports arm64 as well amd64
- like using pipenv instead of pip, pipenv verifies dependency package hashes, this leads to slightly more security
- refactor out of Makefile scripts like spark-submit and build-reqs.sh
- using pytest rather than unittest


Note: Using PEX and common [entrypoint.py](entrypoint.py) to run jobs is quite beneficial, 
jobs can be packed/modularised and easily shipped to production with minimum dependencies.

Things to improve:
 - Better test code coverage for newday job
 - Move test into separate folder within job, so all jobs will be isolated and easily removed in future with simple folder removal
 - Add auto liter/style guide validation/test coverage report
 - Better dataset schema management without using fixed url in code
 - Better job arguments handling, validation, allowed types, etc


