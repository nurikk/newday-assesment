# Newday spark assesment.

I'm not a spark developer and don't write spark jobs on a daily basis,
to get most production ready solution it's better to rely on exising boilerplates. 
Quick googling on "pyspark job boilerplate" gives number of projects/samples to follow.

https://github.com/ekampf/PySpark-Boilerplate
    - quite old
    - manipulates with sys.path.insert
    - manually packages zip with dependencies
    - quite basic test samples

https://medium.com/@shangmin.j.jiang/pyspark-job-template-with-pex-d7de7236707e
    + dockerized solution
    + jobs as packages
    + uses PEX format
    + decent test samples
    - manual dependencies packaging, even though PEX

https://github.com/pratikbarjatya/pyspark-boilerplate-template
    - quite similar to ekampf boilerplate

https://github.com/rodalbuyeh/pyspark-k8s-boilerplate
    + k8s
    + docker
    + build with developer workflow in mind    
    + quite interesting approach, running spark on k8s rather that dedidcated spark cluster
    - GCP only example, but can be refactored for AWS EKS, Azure AKS
    - a bit overkill for a interview assessment

It's made decision to use example from @shangmin.j.jiang with 
some changes like using pipenv instead of vanilla requirements.txt (pip) due to security concerns (pipenv validates all package hashes, pip doesn't)


