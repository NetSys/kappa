---
layout: default
title: index
---
{{ site.name }} automates the task of porting application code to run in
serverless environments, and provides developers with concurrency mechanisms
that enable parallel computation and coordination in these environments.

## Get {{ site.name }}
```console
user:./$ mkdir kappa_home; cd kappa_home
user:./kappa_home$ wget {{ site.url }}{{ site.baseurl }}/kappa
user:./kappa_home$ chmod +x kappa
```
{{ site.name }} comes in a single Bash script, `kappa`, which is responsible
both for downloading {{ site.name }} and executing {{ site.name }} applications.

