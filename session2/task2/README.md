
build
-----

`mvn clean install -U`


run
---


upload jar on hive node


`ADD JAR /home/cloudera/hive2.jar;`


`CREATE TEMPORARY FUNCTION agent_pars_udf as 'com.dstepanova.session2.task2.UserAgentFunc';`


`SELECT agent_pars_udf('Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405')`


btw indecies are in notebook.sql