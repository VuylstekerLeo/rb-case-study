# rb-case-study

The project is a data pipeline use case on soccer data.

## Getting Started

### Project architecture

```
├── README.md                       <- The top-level README for developers using this project.
│
├── requirements.txt                <- The requirements file for reproducing the analysis environment.
│
├── .gitignore
│
├── reports                         <- Data dictionaries, manuals, and all other visualization materials.
│
├── src                             <- Source code for use in this project.
│   ├── club_elo_dag.py             <- Simple DAG for club_elo ingestion
│   │
│   └── workloads                   <- ETL folder
│       ├── quality                 <- ETL folder
│       │   └── quality/schemas     <- Schema folder for linting
│       ├── calculated_summary.py
│       ├── club_elo.py
│       ├── club_elo_api.py
│       └── spadl_actions.py
│
├── test                      <- Tests folder.
│
├── pytest.ini                <- Pytest config
│
├── .coveragerc               <- code coverage config
│
└── .pylinctrc                <- pylint configuration for all contributors.
```

### Installation

We used Python 3.10 for this project.

Additionally, we recommend setting up a Python virtual environment to manage dependencies. You can create a virtual environment using the following command:

```bash
python3 -m venv venv
```

Activate the virtual environment:
On macOS and Linux:
```bash
source venv/bin/activate
```
On Windows:
```bash
venv\Scripts\activate
```
Once the virtual environment is activated, you can install the required Python packages using pip:
```bash
pip install -r requirements.txt
```

### Test

Locally, you can check your setup phase correctness by running some of the tests. For instance:
```
pylint --recursive=y ./
python -m pytest --capture=tee-sys -o junit_logging=all --junit-xml=junit/test-results.xml tests/
```
