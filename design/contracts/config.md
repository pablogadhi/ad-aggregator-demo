# Configuration matrix

Which connection contracts and env vars each workload gets (written by the architect).
`connections:` entries become env vars with a prefix: `postgres` -> `POSTGRES_URL`, ...
(keys per component: infra/components/AUTHORING.md).

| Workload | connections | extra env | route |
| -------- | ----------- | --------- | ----- |
| client   | —           | —         | `/`   |
