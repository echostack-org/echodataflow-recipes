# echodataflow-recipes

This repository contains deployment recipes and helper scripts for running
Echodataflow workflows with Prefect.

A recipe defines:

- which Echodataflow flows should be deployed;
- the parameters passed to each flow;
- how flows are scheduled or triggered;
- the local paths and external resources used by a workflow.

The same deployment structure can be reused for different processing workflows.
For example, the workflow included here (CPS) uses the generic monitoring,
processing-ledger, and event infrastructure, but downstream processing can be
adapted for other tasks such as noise removal, MVBS generation,
classification, or other mission-specific processing.


## Quick start

1. Configure `recipes/params/params_<workflow>.yaml`.
2. Start the Prefect server.
3. Start a Prefect worker.
4. Start the required filesystem monitor(s).
5. Deploy the recipe with `echodataflow-deploy`.
6. Monitor runs in the Prefect UI.

## Repository structure

```text
recipes/
├── deploy/      # Prefect deployment definitions: schedules, triggers,
│                # deployment names, and work-pool configuration
├── params/      # flow-specific parameters: paths, sonar settings,
│                # processing options, thresholds, etc
└── resources/   # optional ancillary files used by particular workflows

scripts/
├── watch_raw_updates.py
│                # watches a RAW-data directory, updates the configured
│                # SQLAlchemy processing ledger, and emits
│                # echodataflow.raw.updated Prefect events
└── watch_transect_updates.py
                 # watches a transect start/end CSV and emits
                 # echodataflow.transect.updated Prefect events

templates/
└── monitoring/
    ├── deploy.yaml
    │            # generic deployment template for an event-driven
    │            # monitoring workflow
    └── params.yaml
                 # generic parameter template to use as a starting point
                 # when creating a new monitoring recipe
```

## Near-real-time workflow architecture

Echodataflow supports event-driven near-real-time processing based on three
components:

1. **Filesystem monitoring** — `watchdog` monitors input files and directories
   and emits Prefect events when relevant changes occur.

2. **Persistent processing state** — a SQLAlchemy-backed processing ledger
   keeps track of available inputs and processing state across flow runs and
   restarts. PostgreSQL is recommended for near-real-time deployments where
   multiple flows may access the ledger concurrently. SQLite remains useful for
   simple local or single-writer workflows.

3. **Event-driven processing** — Prefect deployments subscribe to filesystem
   events such as `echodataflow.raw.updated` or `echodataflow.transect.updated`,
   and can also use native Prefect flow-run events for flow-to-flow orchestration.

For example:

```text
RAW file arrives
      │
      ▼
RAW watcher
      │
      ├── update processing ledger
      └── emit echodataflow.raw.updated
                         │
                         ▼
                    raw2Sv_CPS
                         │
                         ▼
              prefect.flow-run.Completed
```

Prefect events act as triggers, while the processing ledger provides
persistent state that flows can use to determine what still needs to be
processed.

For database-backed near-real-time workflows such as CPS, this replaces
repeatedly scanning directories or using CSV processing lists as the main
orchestration ledger. Existing generic workflows may continue to use their
established CSV-ledger architecture.

Scheduled flows are still useful for operations such as simulated data arrival,
test workflows, or periodic cache updates.

Additional inputs can be monitored in the same way. For example, a transect
definition file can emit:

```text
echodataflow.transect.updated
```

Downstream flows can listen to one or several events:

```text
prefect.flow-run.Completed ───┐
                              ├── downstream processing flow
echodataflow.transect.updated ┘
```

The event triggers the flow, while the current inputs and processing ledger
provide the state used to decide what work remains.

## Working directory structure

Each deployment uses a workflow root directory. This directory contains the
input data, persistent processing state, generated products, and optional
visualization cache.

For the CPS workflow, a typical working directory is:

```text
cps_workflow/
├── raw/                                      # incoming RAW files
├── Sv/                                       # generated calibrated Sv products
├── CPS_Masks_Zarr/                           # generated CPS masks
├── CPS_NASC_Zarr/                            # generated NASC Zarr products
├── CPS_NASC_CSV/                             # generated NASC CSV products
├── CPS_Seafloor_CSVs/                        # generated seafloor estimates
├── viz_cache_CPS/                            # generated visualization cache
│
├── Sv_files.csv                              # auxiliary/legacy Sv file index
├── plotSurvey_Survey_Data_Visualizer.csv     # transect start/end information
└── plotSurvey_Survey_Data_Visualizer_snapshot.csv
                                               # previous transect state
```

Most processing outputs are created by the workflow as needed.

For a live deployment, the main inputs are typically:

- a directory where RAW files arrive;
- a workflow root directory (`path_main`);
- a processing database configured with `processing_db`;
- a transect start/end CSV if the downstream workflow depends on transects.

The processing database does not need to live inside the workflow directory.
For PostgreSQL, `processing_db` is a SQLAlchemy database URL. For SQLite, it can
still be a local database path.

The exact paths and database connection are configured in the parameter recipe.

For example:

```yaml
flows:
  raw2Sv:
    path_main: "/path/to/cps_workflow"
    processing_db: "postgresql+psycopg://USER:PASSWORD@HOST:5432/DATABASE"

  process_CPS:
    path_main: "/path/to/cps_workflow"
    path_transect_csv: "/path/to/cps_workflow/plotSurvey_Survey_Data_Visualizer.csv"
    path_snapshot_csv: "/path/to/cps_workflow/plotSurvey_Survey_Data_Visualizer_snapshot.csv"
    processing_db: "postgresql+psycopg://USER:PASSWORD@HOST:5432/DATABASE"
```

## Processing ledger and file indexes

The near-real-time workflow uses a SQLAlchemy-backed processing ledger.

For concurrent near-real-time processing, PostgreSQL is the recommended backend.
A typical connection string is:

```text
postgresql+psycopg://USER:PASSWORD@HOST:5432/DATABASE
```

SQLite is also supported for simpler local or single-writer workflows. In that
case, `processing_db` can be a local database path such as:

```text
processing.db
```

The ledger provides persistent workflow state independently of Prefect events.
Filesystem events are notifications rather than the source of truth. A flow can
query the processing database to determine which inputs or products still need
processing, including after a restart or when upstream products arrive later
than expected.

Using SQLAlchemy keeps the workflow logic independent of the database backend,
while PostgreSQL provides safer concurrent access when several near-real-time
flows may read from or write to the ledger at the same time.

This also allows downstream flows to reconcile asynchronously arriving inputs.

Older Echodataflow workflows used CSV files such as:

```text
Sv_files.csv
```

as processing ledgers and lookup tables.

`Sv_files.csv` may still be produced for compatibility or auxiliary use, but it
is no longer the primary source of workflow state in the database-backed
near-real-time architecture.

New near-real-time workflows should use the configured `processing_db` for
orchestration and reconciliation rather than relying on `Sv_files.csv`.

## 1. Create a recipe

A workflow normally uses two YAML files:

```text
recipes/deploy/deploy_<workflow>.yaml
recipes/params/params_<workflow>.yaml
```

### Deployment configuration

The deployment file defines when or why each flow should run.

For example:

```yaml
flows:
  raw2Sv:
    deployment_name: raw2Sv-example
    triggers:
      - expect: "echodataflow.raw.updated"
        resource_name: "raw-monitor"
        resource_scope: primary
```

Here, `raw2Sv` is event-driven: it runs when the RAW monitor reports a change,
rather than being repeatedly scheduled to check for new data.

A downstream processing flow can similarly listen for completion of an
upstream flow:

```yaml
flows:
  my_processing_flow:
    deployment_name: my-processing-example
    triggers:
      - expect: "prefect.flow-run.Completed"
        resource_name: "raw2Sv-example"
        resource_scope: related
```

A flow may also listen for several independent events when it needs to reconcile
multiple inputs.

### Parameter configuration

The parameter file contains the arguments passed to each flow.

For example:

```yaml
flows:
  raw2Sv:
    path_main: "/path/to/workflow"
    processing_db: "postgresql+psycopg://USER:PASSWORD@HOST:5432/DATABASE"

    encode_mode: power
    waveform_mode: CW
    sonar_model: EK80
```

Paths and mission-specific processing parameters should normally be changed in
this file without changing the generic deployment infrastructure.

## 2. Start Prefect

Start a local Prefect server:

```bash
prefect server start
```

Then, in another terminal, point Prefect clients to the local server.

On Linux/macOS:

```bash
export PREFECT_API_URL="http://127.0.0.1:4200/api"
```

On PowerShell:

```powershell
$env:PREFECT_API_URL="http://127.0.0.1:4200/api"
```

Start a worker:

```bash
prefect worker start --pool local
```

## 3. Start the monitors

The monitors run independently from the Prefect worker.

### RAW file monitor

```bash
python scripts/watch_raw_updates.py \
    /path/to/workflow/raw \
    --db-path "postgresql+psycopg://USER:PASSWORD@HOST:5432/DATABASE"
```

The RAW monitor:

1. watches the RAW directory for new or updated files;
2. records their state in the configured SQLAlchemy processing ledger;
3. emits an `echodataflow.raw.updated` Prefect event when relevant changes occur.

`--db-path` accepts either a local SQLite path or a SQLAlchemy database URL.
For concurrent near-real-time use, PostgreSQL is recommended.

### Transect monitor

For workflows that depend on an external transect start/end file:

```bash
python scripts/watch_transect_updates.py \
    /path/to/workflow/plotSurvey_Survey_Data_Visualizer.csv
```

Relevant changes emit:

```text
echodataflow.transect.updated
```

Not every workflow requires a transect monitor.

## 4. Deploy the recipe

Recipes are deployed with:

```bash
echodataflow-deploy run \
  --default-work-pool-name local \
  --param-config REPO_DIRECTORY/recipes/params/params_MISSION_NAME.yaml \
  --deploy-spec REPO_DIRECTORY/recipes/deploy/deploy_MISSION_NAME.yaml
```

For example:

```bash
echodataflow-deploy run \
  --default-work-pool-name local \
  --param-config recipes/params/params_cps_test.yaml \
  --deploy-spec recipes/deploy/deploy_cps_test.yaml
```

Once deployed, Prefect handles scheduled and event-triggered flow execution.

## Transect input

Workflows that require transect information expect a transect start/end CSV.

For a live deployment, this file can be written or updated by the acquisition or
survey system.

For testing, Echodataflow provides `simulate_transects`, which progressively
creates and updates the transect file to simulate near-real-time survey
progress.

For example:

```yaml
flows:
  simulate_transects:
    path_transect_csv: "/path/to/cps_workflow/plotSurvey_Survey_Data_Visualizer.csv"
    survey_start: "2024-08-05T13:16:00+00:00"
    transect_duration_minutes: 20
    start_transect_num: 1
    max_transects: 20
```

The transect watcher monitors this file and emits:

```text
echodataflow.transect.updated
```

when its contents change.

The snapshot file:

```text
plotSurvey_Survey_Data_Visualizer_snapshot.csv
```

stores the previously observed transect state so newly completed or updated
transects can be identified.

## Simulating RAW data arrival

For testing or replaying historical data, the `copy_raw` flow can progressively
copy RAW files into the watched `raw/` directory.

It uses a CSV describing the available source files, for example:

```text
SH2407_raw_list_organized.csv
```

with fields such as:

```text
date
time
size
s3_path
timestamp
```

A corresponding parameter recipe can define the source list, destination
directory, and replay time range:

```yaml
flows:
  copy_raw:
    path_raw_list: "/path/to/SH2407_raw_list_organized.csv"
    path_copy: "/path/to/cps_workflow/raw"
    s3_bucket: noaa-wcsd-pds
    exclude_before: "2024-08-05T13:10:00+00:00"
    exclude_after: "2024-08-06T12:30:00+00:00"
```

This simulation layer is optional. In a live deployment, RAW files simply arrive
in the watched directory from the acquisition system.

## CPS example

The CPS recipe is a complete example built on the generic monitoring,
processing-ledger, and event infrastructure.

```text
RAW files arrive
(or copy_raw for testing)
        │
        ▼
   RAW watcher
        │
        ▼
echodataflow.raw.updated
        │
        ▼
    raw2Sv_CPS
        │
        ▼
prefect.flow-run.Completed ───────────┐
                                      │
transect file                         │
        │                             │
        ▼                             │
transect watcher                      │
        │                             │
        ▼                             │
echodataflow.transect.updated ────────┤
                                      ▼
                                  process_CPS
                                       │
                                       ├── CPS masks
                                       ├── seafloor estimates
                                       ├── NASC Zarr
                                       └── NASC CSV
                                       │
                                       ▼
                                update_cache_CPS
                                       │
                                       ▼
                                  visualization
```

The corresponding recipe files are:

```text
recipes/deploy/deploy_cps_test.yaml
recipes/params/params_cps_test.yaml
```

The CPS workflow therefore serves both as a runnable workflow and as an example
for building other event-driven Echodataflow recipes.

### Using the CPS example

The CPS test recipe includes utilities for replaying historical data as though
they were arriving in near real time.

These include:

- `copy_raw`, which can be launched manually to progressively copy RAW files into
  the watched directory;
- `simulate_transects`, which periodically updates a transect start/end file;
- `update_cache_CPS`, which periodically refreshes the visualization cache.

For testing, `copy_raw` can be started manually with:

```bash
prefect deployment run "flow-copy-raw/copy-raw-cps-test"
```

These components are useful for testing and replay. In a live deployment, RAW
files and transect information may instead arrive from the actual acquisition
system.

Users should adapt `params_cps_test.yaml` for the target deployment, in
particular:

- workflow paths;
- the `processing_db` SQLAlchemy connection string;
- RAW data source and replay time range, when applicable;
- sonar configuration;
- transect input;
- target frequency;
- CPS and seafloor-processing parameters.

The deployment file mainly describes orchestration: which flows are scheduled,
which flows are event-driven, and which Prefect events trigger them.

## CPS visualization

The CPS visualization uses the workflow root to locate the processing database,
CPS products, NASC products, transect information, and visualization cache.

Set the workflow root before starting the visualization.

On Linux/macOS:

```bash
export ECHODATAFLOW_CPS_ROOT="/path/to/cps_workflow"
```

On PowerShell:

```powershell
$env:ECHODATAFLOW_CPS_ROOT="C:\path\to\cps_workflow"
```

The visualization therefore does not depend on machine-specific hard-coded
paths: its file inputs are resolved relative to the configured workflow root.

When the processing ledger is not the default local SQLite file, configure the
database connection explicitly as well.

On Linux/macOS:

```bash
export ECHODATAFLOW_CPS_PROCESSING_DB="postgresql+psycopg://USER:PASSWORD@HOST:5432/DATABASE"
```

On PowerShell:

```powershell
$env:ECHODATAFLOW_CPS_PROCESSING_DB="postgresql+psycopg://USER:PASSWORD@HOST:5432/DATABASE"
```

The default target frequency is 70 kHz and can optionally be changed with:

On Linux/macOS:

```bash
export ECHODATAFLOW_CPS_TARGET_FREQUENCY="70000"
```

On PowerShell:

```powershell
$env:ECHODATAFLOW_CPS_TARGET_FREQUENCY="70000"
```

### Start the visualization

From the `echodataflow` repository, with the Echodataflow environment activated,
start the CPS monitoring application directly with Python:

```bash
python src/echodataflow/services/viz_echogram_track_cps.py
```

For example, on PowerShell:

```powershell
conda activate echodataflow

$env:ECHODATAFLOW_CPS_ROOT="C:\path\to\cps_workflow"
$env:ECHODATAFLOW_CPS_PROCESSING_DB="postgresql+psycopg://USER:PASSWORD@HOST:5432/DATABASE"
$env:ECHODATAFLOW_CPS_TARGET_FREQUENCY="70000"

python src\echodataflow\services\viz_echogram_track_cps.py
```

The dashboard is then available at:

```text
http://127.0.0.1:1803/cps_echogram
```

The dashboard reads the latest visualization cache produced by
`update_cache_CPS`:

```text
process_CPS
    │
    ▼
update_cache_CPS
    │
    ▼
viz_cache_CPS/latest_CPS.zarr
    │
    ▼
CPS monitoring dashboard
```

`update_cache_CPS` must have produced a valid cache before processed CPS data can
be displayed.
