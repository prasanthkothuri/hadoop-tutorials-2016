
JAVA_HOME=/usr/lib/jvm/jre-openjdk

# Give Flume more memory and pre-allocate, enable remote monitoring via JMX
JAVA_OPTS="-Xms20m -Xmx50m -Duser.timezone=Europe/Zurich"

# Limit vmem usage
# https://issues.apache.org/jira/browse/HADOOP-7154
export MALLOC_ARENA_MAX=4