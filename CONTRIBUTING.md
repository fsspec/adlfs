Dask is a community maintained project. We welcome contributions in the form of bug reports, documentation, code, design proposals, and more.

For general information on how to contribute see https://docs.dask.org/en/latest/develop.html.

To develop ``adlfs`` it best to work in an virtual environment.
You can create a virtual environment using ``conda`` and install the development dependencies as follows:

```bash
$ conda create -n adlfs-dev python=3.8
$ conda activate adlfs-dev
$ pip install -r requirements/latest.txt
```

You can run tests from the main directory as follows:
```bash
$ py.test adlfs/tests
```

## Release

```
# Update CHANGELOG.md
# Create a tag. Should start with v
git commit --allow-empty -m 'RLS: <tag>'
git tag -a -m 'RLS: <tag>' <tag>
git push upstream main --follow-tags
python setup.py sdist bdist_wheel
twine upload dist/*
```

## Pytest
Testing `adlfs` requires Microsoft `azurite`, which 
requires `docker`.  Locally, this can be done with `Docker`, or
`colima`, which itself supports the `Docker runtime`.

If using `colima` do:
```
colima start
export DOCKER_HOST="unix://$HOME/.colima/docker.sock"
pytest
```