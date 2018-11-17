# Copyright 2018 ASI Data Science
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
    name="sherlockml",
    description="Python library for interacting with SherlockML.",
    url="https://sherlockml.com",
    author="ASI Data Science",
    author_email="engineering@asidatascience.com",
    license="Apache Software License",
    packages=find_packages(),
    use_scm_version={"version_scheme": "post-release"},
    setup_requires=["setuptools_scm", "pytest-runner"],
    tests_require=[
        "pytest",
        "pytest-mock",
        "requests_mock",
        "python-dateutil>=2.7",
    ],
    install_requires=[
        "requests",
        "pytz",
        "six",
        "enum34; python_version<'3.4'",
        "marshmallow==3.0.0b12",
        "marshmallow_enum",
        "boto3",
        "botocore",
    ],
    dependency_links=[
        "git+https://github.com/marshmallow-code/marshmallow"
        "@3.0.0b12#egg=marshmallow"
    ],
)
