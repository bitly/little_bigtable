# Little Bigtable

![CI Status](https://github.com/bitly/little_bigtable/actions/workflows/test.yaml/badge.svg?branch=master)

A local emulator for [Cloud Bigtable](https://cloud.google.com/bigtable) with persistence to a sqlite3 backend.

The Cloud SDK provided `cbtemulator` is in-memory and does not support persistence which limits it's applicability. This project is a fork of `cbtemulator` from [google-cloud-go/bigtable/bttest](https://github.com/googleapis/google-cloud-go/tree/c46c1c395b5f2fb89776a2d0e478e39a2d5572e4/bigtable/bttest)

| | [`cbtemulator`](https://cloud.google.com/bigtable/docs/emulator) | "little" Bigtable | Bigtable
| --- | ----- | ---- | ----
| **Storage** | In-Memory | sqlite3 | Distributed GFS
| **Type** | Emulator | Emulator | Managed Production Datastore
| **Scaling**| Single process | Single process | Scalable multi-node backend
| **GC** | async GC | per-row GC at read time |

## Usage

```
Usage of ./little_bigtable:
  -db-file string
      path to data file (default "little_bigtable.db")
  -host string
      the address to bind to on the local machine (default "localhost")
  -port int
      the port number to bind to on the local machine (default 9000)
  -version
      show version
```

In the environment for your application, set the `BIGTABLE_EMULATOR_HOST` environment variable to the host and port where `little_bigtable` is running. This environment variable is automatically detected by the Bigtable SDK or the `cbt` CLI. For example:

```bash
$ export BIGTABLE_EMULATOR_HOST="127.0.0.1:9000"
$ ./run_my_app
```

## Limitations

Some filters are not implemented or have partial support. See [cbtemulator docs](https://cloud.google.com/bigtable/docs/emulator#filters)

