# finops-database-handler

A webservice proxy for all requests to Krateo's CrateDB instance, enabling data upload, querying, and notebook-based computation over stored FinOps data.

📖 **Full documentation**: [docs.krateo.io — finops-database-handler](https://docs.krateo.io/key-concepts/kcf/finops-components/finops-database-handler/)

---

## Key features

- Proxies all database operations through a single authenticated HTTP endpoint with Kubernetes RBAC-controlled access
- Supports chunked data uploads and SQL-based querying of FOCUS cost and resource metrics
- Executes agnostic Python notebooks against the database for custom computations and data output

## Requirements

| Dependency | Minimum version |
|------------|----------------|
| Kubernetes | v1.31 |
| Krateo | v3.0.0 |
| CrateDB | v5.9.6 |

## Install

```bash
helm repo add krateo https://charts.krateo.io
helm repo update
helm install finops-database-handler krateo/finops-database-handler --namespace krateo-system --create-namespace
```

> For advanced installation options, custom values, and upgrade instructions, see the [installation guide](https://docs.krateo.io/key-concepts/kcf/finops-components/finops-database-handler/).

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `POLLING_INTERVAL` | No | `300` | Polling interval of the operator in seconds |
| `MAX_RECONCILE_RATE` | No | `1` | Number of workers for the operator |