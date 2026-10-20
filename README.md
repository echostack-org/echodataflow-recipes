# Echodataflow recipes

This repository contains runnable workflows for [Echodataflow](https://github.com/echostack-org/echodataflow), together with [Echoshader](https://github.com/echostack-org/echoshader) visualization configurations.

## Acoustic template

`recipes_nrt/template/` provides a reusable starting point for processing acoustic data and visualizing its outputs.

| File | Purpose |
| --- | --- |
| `deploy.yaml` | Prefect deployment definitions for `raw2Sv` and `create_MVBS` |
| `params.yaml` | Processing parameters, input/output paths, and MVBS binning |
| `acoustic_template.yaml` | Generic Echoshader Panel application configuration |

The workflow processes four consecutive NOAA EK80 raw files through `raw2Sv` and `create_MVBS`, then opens a resulting MVBS Zarr dataset with Echoshader.

### Run the interactive Echoshader app locally

Install Echodataflow and Echoshader, ensure the template MVBS output exists, then run from the repository root:

```bash
python -m echoshader.app.launcher recipes_nrt/template/acoustic_template.yaml
```

By default, the application is available at [http://localhost:1807/](http://localhost:1807/).

The `data.survey.path` setting in `acoustic_template.yaml` specifies the Zarr dataset to visualize. Paths are resolved relative to the YAML file when using the Echoshader launcher. You can replace this path with your own processed acoustic dataset.

### CI and GitHub Pages

The GitHub Actions workflow `.github/workflows/recipe-template.yml`:

- Downloads four consecutive NOAA EK80 raw files.
- Starts a local Prefect server and worker.
- Deploys and runs the `raw2Sv` and `create_MVBS` flows.
- Verifies the generated MVBS Zarr output.
- Builds the generic Echoshader application.
- Exports an HTML preview to `.local/pages/acoustic-template/index.html`.

The workflow runs on pull requests and publishes the preview through GitHub Pages on pushes to `main`.

**Note:** GitHub Pages hosts a static HTML export, not a running Python application. Full Panel interactivity requires launching Echoshader with a Python server.

## Other recipes

`recipes_nrt/monitoring/` contains the monitoring recipe. CPS-specific developments are handled independently of the generic acoustic template.
