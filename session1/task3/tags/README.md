build
-----

mvn clean install -U


run
---

YARN_OPTS="-Dsrc=... -Denrich=/user/dstepanova/user.profile.tags.us.txt -Dout=..." yarn  jar ./tags.jar

