## <ins>wav3_</ins>

The goal of this project is to create a deverse data platform whose deployment is fully automated

&nbsp;

### <ins>DEPLOYMENT:</ins>

The setup of the below VMs is completely automated using the Ansible playbooks

Virtual Machines:

1.  Kafka broker running in a docker container
2.  Redis server running in a docker container
3.  (Multiple - 3?) Clickhouse cluster running directly on vm
4.  (Multiple - 3?) Clickhouse keeper cluster running directly on vm
5.  Data producers - see "Producers" below
6.  Data consumers - see "Consumers" below
7.  Web server to visualize data (and allow for producer data manipulation?)

&nbsp;

### <ins>PRODUCERS:</ins>

- Kafka - Writes random values as event stream to Kafka broker
- Redis - Updates values of keys to random values on some cadence

&nbsp;

### <ins>CONSUMERS:</ins>

- Different languages consumers
    1.  Read from Kafka event stream
    2.  Read key values from Redis
    3.  Do some calculation with these values
    4.  Output calculation results as timeseries to Clickhouse

&nbsp;

- Visualization (web server?)

	1.  Read the data from Clickhouse and create some sort of visualization
    2.  Allow for direct manipulation of data producers for interactivity
