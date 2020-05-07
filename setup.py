import os

from setuptools import (
    find_packages,
    setup
)

with open(os.path.join('requirements.notebook.txt'), 'r') as f:
    REQUIRED_PACKAGES = f.readlines()

packages = [x for x in find_packages()
            if x not in {'dags', 'tests'}]

setup(
    name='data_science_notebook_dags',
    version='0.0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=packages,
    include_package_data=True,
    description='Data Science Notebook DAGs',
)
