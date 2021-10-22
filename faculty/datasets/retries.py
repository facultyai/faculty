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

"""Request retry handling"""


import random
import sys


def maybe_retry(
    make_request,
    max_attempts=10,
    max_backoff=20,
    retryable_codes=None,
):
    if retryable_codes is None:
        retryable_codes = []

    n_attempts = 0
    response = make_request()
    while (
        response.status_code in retryable_codes and n_attempts < max_attempts
    ):
        n_attempts += 1
        delay = min(get_exponential_backoff(n_attempts), max_backoff)
        sys.sleep(delay)
        response = make_request()

    return response


def get_exponential_backoff(attempt_number):

    return random.random(0, 1) * 2 ** (attempt_number - 1)
