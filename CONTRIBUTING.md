Dask is a community maintained project. We welcome contributions in the form of bug reports, documentation, code, design proposals, and more. 

For general information on how to contribute see https://docs.dask.org/en/latest/develop.html.

To develop ``adlfs`` it best to work in an virtual environment.
You can create a virtual environment using ``conda`` and install the development dependencies as follows:

```bash
$ conda create -n adlfs-dev python=3.8
$ conda activate adlfs-dev
$ pip install -r dev-requirements.txt
```

You can run tests from the main directory as follows:
```bash
$ py.test adlfs/tests
```
