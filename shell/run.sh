#!/bin/sh

mvn package assembly:single -T 16C
mv target/orca-1.0-SNAPSHOT-jar-with-dependencies.jar orca.jar

java -jar orca.jar replay \
  ./example/tidb_42487/config_tidb_42487.yaml \
  ./example/tidb_42487/out \
  ./example/tidb_42487/replay