import click
import pytest
from click.testing import CliRunner

from faculty.cli.cli import cli
from faculty.clients.project import ProjectClient, ProjectSchema
from tests.fixtures import PROJECT, USER_ID


@pytest.fixture
def mock_update_check(mocker):
    mocker.patch("faculty.cli.update.check_for_new_release")


@pytest.fixture
def mock_check_credentials(mocker):
    mocker.patch("faculty.cli.cli._check_credentials")


@pytest.fixture
def mock_profile(mocker):
    mocker.patch("faculty.cli.auth.user_id", return_value=USER_ID)


def test_list_projects(
    mocker, mock_update_check, mock_check_credentials, mock_profile
):
    runner = CliRunner()
    schema_mock = mocker.patch("faculty.clients.project.ProjectSchema")
    mocker.patch.object(ProjectClient, "_get", return_value=[PROJECT])

    result = runner.invoke(cli, ["projects"])

    assert result.exit_code == 0
    assert result.output == f"{PROJECT.name}\n"

    ProjectClient._get.assert_called_once_with(
        "/user/{}".format(USER_ID), schema_mock.return_value
    )
