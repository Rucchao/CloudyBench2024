# CloudyBench - A Benchmark for Cloud-Native Databases 

## Supporting P-Score calculation as follows:

```
-t runReplica -c conf/pg.props -f conf/stmt_postgres.toml -m 1
```

## Supporting E1-Score calculation as follows:

```
-t runElastic -c conf/pg.props -f conf/stmt_postgres.toml -m 1
```

## Supporting R-Score and F-Score calculation as follows:

```
-t runFailOver -c conf/pg.props -f conf/stmt_postgres.toml -m 2
```

## Supporting E2-Score calculation as follows:

```
-t runScaling -c conf/pg.props -f conf/stmt_postgres.toml -m 1
```

## Supporting C-Score calculation as follows:

```
-t runLagTime -c conf/pg.props -f conf/stmt_postgres.toml -m 1
```

## Supporting T-Score calculation as follows:

```
-t runTenancy -c conf/pg.props -f conf/stmt_postgres.toml -m 3
```
