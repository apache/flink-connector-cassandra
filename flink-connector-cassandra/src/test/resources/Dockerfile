FROM cassandra:4.0.8

ENV LOCAL_JMX no
COPY jmxremote.password /etc/cassandra/jmxremote.password
COPY jmxremote.access /etc/cassandra/jmxremote.access
RUN chown cassandra:cassandra /etc/cassandra/jmxremote.*
RUN chmod 400 /etc/cassandra/jmxremote.*

