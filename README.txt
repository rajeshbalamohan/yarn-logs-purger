Simple command line utility to purge yarn aggregated logs.

Package:
=======
mvn clean package

Usage:
======
1. List all logs that needs to be deleted older than 300 days.
yarn jar ./target/yarn-logs-purger-1.0-SNAPSHOT.jar -DdeleteOlderThan=300

2. List & Delete all logs that are older than 300 days
yarn jar ./target/yarn-logs-purger-1.0-SNAPSHOT.jar -DdeleteOlderThan=300 -DdeleteFiles=true

It would also print the space savings in HDFS at the end of the run.

Note: Not multi-threaded. If there are huge number of logs, it might take longer time. Ideally,
timing can be reduced with multiple threads, but for initial version this is sufficient.