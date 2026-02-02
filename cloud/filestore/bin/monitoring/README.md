### Monitoring for the local setup

This is a simple config for monitoring the local setup described in the [cloud/filestore/bin/README.md](../../README.md).

To start Grafana + Prometheus, run:

```bash
docker compose up -d
```

To stop the monitoring stack, run:

```bash
docker compose down
```

Grafana will be available at [http://localhost:3000](http://localhost:3000) (default login/password: `admin`/`admin`).
