# How to run tests

### To generate the schema

```
(use 'sqlforcql.schema)
(sqlforcql.schema/generate-schema)
```

### For running read (R) tests

```
(clojure.test/run-tests 'sqlforcql.select-test)
```

### For running update (U) tests

```
(clojure.test/run-tests 'sqlforcql.update-test)
```

### Old tests
```
(clojure.test/run-tests 'sqlforcql.core-test)
```
