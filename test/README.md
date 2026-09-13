# Tests

e.g.:

```
coverage run -m unittest test_fs_local.py
coverage html
```

Run the lookup-join regressions from the repository root with project dependencies
installed; these tests do not connect to Kafka:

```sh
python -m unittest test.streams.test_lookup_join
```
