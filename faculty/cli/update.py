"""Prompt the user to update faculty"""

# Copyright 2016-2018 ASI Data Science
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

import errno
import os
import time
from distutils.version import StrictVersion

import click
import requests
import faculty.cli.version


def _ensure_parent_exists(path):
    directory = os.path.dirname(path)
    try:
        os.makedirs(directory)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise


def _set_mtime(path):
    _ensure_parent_exists(path)
    if os.path.exists(path):
        os.utime(path)
    else:
        open(path, "a").close()


def _last_update_path():
    xdg_cache_dir = os.environ.get("XDG_CACHE_DIR")

    if not xdg_cache_dir:
        xdg_cache_dir = os.path.expanduser("~/.cache")

    return os.path.join(xdg_cache_dir, "faculty", "last_update_check")


def _get_pypi_versions():
    response = requests.get("https://pypi.python.org/pypi/sml/json", timeout=1)
    versions = response.json()["releases"].keys()
    return [StrictVersion(v) for v in versions]


def _check_for_new_release():
    current = StrictVersion(faculty.cli.version.__version__)
    latest = max(_get_pypi_versions())
    if current < latest:
        template = (
            "You are using faculty version {}, however version {} is "
            "available.\n"
            "You should upgrade with 'pip install --upgrade faculty'."
        )
        click.secho(template.format(current, latest), err=True, fg="yellow")
    _set_mtime(_last_update_path())


def check_for_new_release():
    """Check for new releases, at most once every day."""
    check_pypi = True

    try:
        last_check_time = os.stat(_last_update_path()).st_mtime
        one_day_ago = time.time() - 86400
        if last_check_time > one_day_ago:
            check_pypi = False
    except OSError:
        pass

    if check_pypi:
        _check_for_new_release()
