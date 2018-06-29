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
    name='sherlockml',
    description='Python library for interacting with SherlockML.',
    url='https://sherlockml.com',
    author='ASI Data Science',
    author_email='engineering@asidatascience.com',
    license='Apache Software License',
    packages=find_packages(),
    setup_requires=[
        'pytest-runner'
    ],
    tests_require=[
        'pytest',
        'pytest-mock',
        'requests_mock'
    ],
    install_requires=[
        'requests',
        'pytz',
        'six',
        'marshmallow<3.0.0'
    ]
)
