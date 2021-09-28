# Little Bigtable

A local emulator for [Cloud Bigtable](https://cloud.google.com/bigtable) with persistance to a sqlite3 backend.

The Cloud SDK provided `cbtemulator` is in-memory and does not support persistance which limits it's applicability. This project is a fork of `cbtemulator` from [google-cloud-go/bigtable/bttest](https://github.com/googleapis/google-cloud-go/tree/c46c1c395b5f2fb89776a2d0e478e39a2d5572e4/bigtable/bttest)

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
```

## Limitations

Some filters are not implemented or have partial support. See [cbtemulator docs](https://cloud.google.com/bigtable/docs/emulator#filters)

