# CloudyBench - A Benchmark for Cloud-Native Databases


## Data Generation, Data Loading and Indexing
```
bash cloudybench -t sql -c conf/pg.props -f conf/ddl_cloudybench_pg.sql

bash cloudybench -t gendata -c conf/pg.props -f conf/stmt_postgres.toml

bash cloudybench -t sql -f conf/load_cloudybench_pg.sql

bash cloudybench -t sql -c conf/pg.props -f conf/create_index_cloudybench_pg.sql
```

## Supporting P-Score calculation as follows (Take Neon as an example):

```
bash cloudybench -t runReplica -c conf/pg.props -f conf/stmt_postgres.toml -m 1
```

## Supporting E1-Score calculation as follows:

```
bash cloudybench -t runElastic -c conf/pg.props -f conf/stmt_postgres.toml -m 1
```

## Supporting R-Score and F-Score calculation as follows:

```
bash cloudybench -t runFailOver -c conf/pg.props -f conf/stmt_postgres.toml -m 1
```

## Supporting E2-Score calculation as follows:

```
bash cloudybench -t runScaling -c conf/pg.props -f conf/stmt_postgres.toml -m 1
```

## Supporting C-Score calculation as follows:

```
bash cloudybench -t runLagTime -c conf/pg.props -f conf/stmt_postgres.toml -m 1
```

## Supporting T-Score calculation as follows:

```
bash cloudybench -t runTenancy -c conf/pg.props -f conf/stmt_postgres.toml -m 3
```

## Supporting O-Score calculation as follows:

```
bash cloudybench -t runAll -c conf/pg.props -f conf/stmt_postgres.toml
```