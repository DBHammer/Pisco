# Pisco

Pisco is a transactional bug reduction framework designed to automatically simplify database anomalies. 
It takes a complex, recorded bug case and generates a minimal and stable bug case, making it easier for developers to debug.

## Getting Started

Follow these steps to build Pisco from source and run it.

### Build
You will need Apache Maven to build the project.
```shell
mvn package assembly:single -T 16C
mv target/orca-1.0-SNAPSHOT-jar-with-dependencies.jar orca.jar
```

### Run Pisco
Use the following command to run the replay module:
```
java -jar orca.jar replay <configDir> <originOutputDir> <newOutputDir>
```
Parameters:
- configDir: Path to the configuration file.
- originOutputDir: The directory containing the original recorded workload.
- newOutputDir: The directory where Pisco will save the minimized replay results.


### Example: Reproducing a TiDB Anomaly
This example is from an anomaly that occurred in TiDB v8.5.0 under the Repeatable Read isolation level. 
Bug Case: https://github.com/pingcap/tidb/issues/42487
For details, please refer to the contents of the `example/tidb_42487/` directory.

#### Step 1: Set up the Environment
Deploy a local instance of TiDB (any version prior to v8.5.1). You can use the official TiDB playground or Docker.

#### Step 2: Configure the Database Connection
Modify the datasource section in example/tidb_42487/config_tidb_42487.yaml with your database credentials:
```yaml
datasource:
  platform: tidb
  url: jdbc:mysql://127.0.0.1:4000/your_database_name?serverTimezone=UTC&useServerPrepStmts=true&cachePrepStmts=true&allowLoadLocalInfile=true
  username: root 
  password: your_password
  driverClassName: com.mysql.cj.jdbc.Driver
```

#### Step 3: Run the Replay
Execute the following command to replay and minimize the bug case. 
Pisco will read the original workload from the out directory and save the reduced case to the replay directory.

```shell
java -jar orca.jar replay \
  ./example/tidb_42487/config_tidb_42487.yaml \
  ./example/tidb_42487/out \
  ./example/tidb_42487/replay
```


### Usage
To see all available commands and parameters, run the JAR file with no arguments:
```shell
java -jar orca.jar
```
This will display the help message:
```
Usage: java -jar $jarfile [replay] ...

Parameters for the 'replay' module:
  configDir         Path to the configuration file.
  originOutputDir   Directory of the original recorded workload.
  newOutputDir      Output directory for the replay results.
```

### Example: Detecting a Duplicate Bug Case

The deduplication process of Pisco is implemented by Python, under the `./deduplicate/` directory.

#### Step 1: Prepare

Before running the deduplication process, please check the Python version (=3.12) and the required dependencies (see requirement.txt).

#### Step 2: Run

Execute the following command to detect the potential duplicate bug reports under the `./deduplicate/` directory. 

```
python3 ./main.py
```

Then, it would output the potential duplicates, e.g., `Most similar report found: tidb_42487`.
