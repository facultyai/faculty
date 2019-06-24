# Copyright 2018-2019 Faculty Science Limited
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


from setuptools import setup, find_packages


setup(
    name="faculty",
    description="Python library for interacting with the Faculty platform.",
    url="https://faculty.ai/products-services/platform/",
    author="Faculty",
    author_email="opensource@faculty.ai",
    license="Apache Software License",
    packages=find_packages(),
    use_scm_version={"version_scheme": "post-release"},
    setup_requires=["setuptools_scm"],
    install_requires=[
        "requests",
        "pytz",
        "six",
        "enum34; python_version<'3.4'",
        # Install marshmallow with 'reco' (recommended) extras to ensure a
        # compatible version of python-dateutil is available
        "attrs",
        "marshmallow[reco]==3.0.0rc3",
        "marshmallow_enum",
        "boto3",
        "botocore",
    ],
    dependency_links=[
        "git+https://github.com/marshmallow-code/marshmallow"
        "@3.0.0rc3#egg=marshmallow"
    ],
)
