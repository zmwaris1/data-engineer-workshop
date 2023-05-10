To run the `ingest_data.py` file with paasing arguments while running:

```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
 --user=root \
 --password=root \
 --host=localhost \
 --port=5432 \
 --db=ny_taxi \
 --table_name=yellow_taxi_trips \
 --url=\${URL}
```

The file can also be run using a Docker conatainer.

To build Docker image with the Python file:

`docker build -t pipeline:latest .`

-t -- serves to assign tag to the container
. -- denotes the working directory to be used to build image

To fire up the conatiner with Python script and ingest data:

```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

docker run -it \
 --network=pg-network \
 taxi_ingest:v001 \
 --user=root \
 --password=root \
 --host=pg-database \
 --port=5432 \
 --db=ny_taxi \
 --table_name=yellow_taxi_trips \
 --url=\${URL}
```

