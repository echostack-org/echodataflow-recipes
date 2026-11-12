# Echodataflow recipes

This repository hosts recipes of runnable workflows for [Echodataflow](https://github.com/echostack-org/echodataflow).

Check out [how Echodataflow works](https://echodataflow.readthedocs.io/en/latest/overview/), and follow the instructions in the [Example pipelines](https://echodataflow.readthedocs.io/en/latest/pipeline-examples/) section to run these examples.

Each recipe folder under `recipes_nrt` or `recipes_postproc` contains deployment and parameter configurations, along with any required resources or visualization settings. `tests/` contains recipe fixtures exercised by CI.


```text
├── .github/                     # GitHub automation
├── drafts/                      # Draft examples
├── recipes_nrt/                 # Near-real-time recipes
│   ├── cps/                     # CPS workflow
│   ├── edge2cloud_hake/         # Edge-to-cloud hake workflow
│   └── spasso/                  # SPASSO workflow
├── recipes_postproc/            # Post-processing recipes
│   └── edge2cloud_hake/         # Hake worfklow
└── tests/                       # CI recipe fixtures
    ├── monitoring/              # Monitoring recipe fixture
    └── template/                # Acoustic template fixture
```
