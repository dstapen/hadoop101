
build
-----

`mvn clean install -U`

launch
------


1. upload input file on HDFS


2. `YARN_OPTS="-Dsrc=/user/dstepanova/hadoop101/user.profile.tags.us.txt -Ddest=/user/dstepanova/hadoop101/result333.txt -Dapp=task2 -Ddeploy=./task2.jar" yarn jar task2.jar`


*src* is a source file on HDFS (required parameter), f.e. /user/dstepanova/hadoop101/user.profile.tags.us.txt


*dest* is a result file on HDFS (required parameter). This file must exist during application invocation.


*app* is an application name (required parameter).


*deploy* is path to particular artifact (required parameter). It exists because of Yarn's RunJar extracts given artifact in directory like application servers do. Can't find a best practice to cope with this problem. That's why such workaround exists.


*mem* is an optional memory limit.


*cpu* is an optional processor cores limit.



other
-----

`hdfs fsck -locations -blocks  /user/dstepanova/hadoop101/user.profile.tags.us.txt`


`hdfs dfs -setrep -w 1 /user/dstepanova/hadoop101/user.profile.tags.us.txt`


`hdfs dfs -ls /user/dstepanova/hadoop101`