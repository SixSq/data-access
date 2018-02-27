# CoTec3

This document provides a description the CoTec3 KPI framework’s development.
This project addresses the data access delay of object storage used in Big Data
environment such as Earth Observation (EO). The objects being accessible only
through a REST API over the HTTP(S) protocol which introduces a slower
throughput than traditional staged data for the processing. The optimization
chosen to reduce this drawback is to parallelize the retrieval and processing
of the data. In addition, a subscribe/notify pattern is put in place to
coordinate efficiently the two part of the pipeline.  Finally, the scope of the
implementation covers only its deployment on a single virtual machine (VM).

The project delivery is on the form of a Python library. The parallelization in
this environment the is done using either concurrent threads on a single core
or by using multiple Linux processes. We use the both in the entire pipeline
respectively the data-retrieval and the processing of EO applications.

## First approach: object Store Data Access technology.
Amazon Web Service(AWS) provides an S3 interface on top of their object
storage. The underneath library is called Boto and is coded in Python. We use
it to implement the following features:

1. Connect/Authenticate to buckets
1. List the objects of buckets
1. Upload objects
1. Download objects

The data retrieval is the part of the pipeline where we can optimize the most
the IO of the system. It is done by using the full bandwidth capacity starting
multiple HTTP threads in parallel. In the Python environment a Global
Interpreter (GIL) avoids multi cores process . Nevertheless, the delay of
switching control between multiple threads is ignorable with respect of the IO
rate.

The users should have the possibility to iterate on EO products through their
application. These collections of object contain one satellite image each and
are stored in directories. They stand in the top level of granularity of the
framework i.e. set of folders. We will parallelize their retrieval by starting
multiple threads concurrently for the three following levels:

1. each products
2. each file/objects
3. each chunks of big objects

By doing so we will always use the full capacity available of the channel
between the data location and the application deployment’s machine. The
optimal number of HTTP connections with respect to object storage maximum
capacity can be obtained using simple performance testing.


## EO processing optimization

The second part of the optimization concerns the parallelization of EO data
processing. In opposition with the data retrieval we can not use threads. Even
if are not necessarily CPU bounded, an EO processing can not be distributed
among multiple cores. However, we use the Python built-in multiprocessing
library to parallelize the different jobs in different, memory separated,
linux processes.

### Subscribe/Notify pattern

The parallelization of the data retrieval and the processing has to be
coordinated to bring a real gain. A core feature has to synchronize the
readiness of the data with the processing. This information can be transmitted
through a shared object. The data objects selected by the user are subscribed
to this shared index which maintains a state for each of them. Next they are
downloaded and pass to the ready state afterwards. A lone process performs the
following steps listed in a chronological order:

1. Register its objects to the shared index.
2. Wait until all the its objects’ pass to the ready state.
3. Execute its processing tasks

When all the jobs get registered, an external process dedicated to the data
download is started. At the end of each object download a callback method
change the associated object’s status on the shared variable. The shared memory
index is a set and avoid duplicated download.

## User experience
We designed the framework to take the form of an attachable to the user’s code.
An running script is provided where the data objects can be listed with their
corresponding processing function and parameters. These on will be take as
arguments at the start of the framework.

The diagram below shows how the framework's entities work with each other
during the execution.
<div style="padding:14px"><img
src="https://github.com/SixSq/data-access/blob/master/Diagram.png"
width="75%"></div>

## Instructions

1. The user should have a Nuvla account.
2. Start the application component 'data-access' from EO-Data-Processing.
   https://nuv.la/module/EO-Data-Processing/data-access
3. Connect via ssh to the started machine, this repos should be already cloned inside it.
4. Put the user S3 credentials' in the *credentials* file and
   copy into the *~/.aws* directory.

  ```
  $ cd data-access
  $ mkdir -p ~/.aws/
  $ cp credentials ~/.aws/
  $ cp config ~/.aws/
  ```

Edit ~/.aws/credentials to set your AWS credentials and if required the
custom S3 endpoint. In case your custom S3 endpoint supports only S3
Sigv2 you should check `config` file for the extra configuration.

5. Run the main script.

  ```
  $ python task_planner.py
  ```
