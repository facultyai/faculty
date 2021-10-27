# Copyright 2018-2021 Faculty Science Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
from setuptools import setup, find_packages


def load_readme():
    path = os.path.join(os.path.dirname(__file__), "README.rst")
    with open(path) as fp:
        content = fp.read()
    return content


setup(
    name="faculty",
    description="Python library for interacting with the Faculty platform.",
    long_description=load_readme(),
    url="https://faculty.ai/products-services/platform/",
    author="Faculty",
    author_email="opensource@faculty.ai",
    license="Apache Software License",
    packages=find_packages(),
    use_scm_version={"version_scheme": "post-release"},
    setup_requires=["setuptools_scm"],
    python_requires=">=3.6",
    install_requires=[
        "requests",
        "pytz",
        "attrs",
        "marshmallow",
        "marshmallow_enum",
        "urllib3",
    ],
)
