## <ins>wav3_</ins>

The goal of this project is to create a diverse data platform whose deployment is fully automated

&nbsp;

### <ins>TECHNOLOGIES</ins>
- <ins>Deployment Automation</ins>: Ansible
- <ins>Event Streaming</ins>: Kafka
- <ins>In Memory DB</ins>: Redis
- <ins>Distributed Relational DB Cluster</ins>: Clickhouse

&nbsp;

### <ins>DEPLOYMENT:</ins>
Ex: [**Num VMs**] - What / how deployed

- [**1**] Kafka broker running in a docker container
- [**1**] Redis server running in a docker container
- [**3?**] Clickhouse cluster running directly on VM
- [**3?**] Clickhouse keeper cluster running directly on VM
- [**n**] Data producers - see "Producers" below
- [**n**] Data consumers - see "Consumers" below
- [**1**] Web server to visualize data (and allow for producer data manipulation?)

&nbsp;

### <ins>PRODUCERS:</ins>

- Kafka - Writes random values as event stream to Kafka broker
- Redis - Updates values of keys to random values on some cadence

&nbsp;

### <ins>CONSUMERS:</ins>

- Different types/language consumers (Python, Spark, Go, Rust?, Elixer?)
    1.  Read from Kafka event stream
    2.  Read key values from Redis
    3.  Do some calculation with these values
    4.  Output calculation results as timeseries to Clickhouse

&nbsp;

- Visualization (web server?)

	1.  Read the data from Clickhouse and create some sort of visualization
    2.  Allow for direct manipulation of data producers for interactivity
