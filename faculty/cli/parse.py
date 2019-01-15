from collections import namedtuple

ESCAPE_CHAR = "\\"

RunIdentifier = namedtuple("RunIdentifier", ["run_number", "subrun_number"])


def parse_run_identifier(string):
    try:
        parts = tuple(map(int, string.split(".", 2)))
    except ValueError:
        raise ValueError("Invalid run identifier: {}".format(string))
    if len(parts) == 1:
        return RunIdentifier(parts[0], None)
    elif len(parts) == 2:
        return RunIdentifier(*parts)
    else:
        raise ValueError("Invalid run identifier: {}".format(string))


def _escape_split(string, delimiter):

    escape_mode = False
    chunk = ""
    parts = []

    for character in string:

        if character == ESCAPE_CHAR:
            escape_mode = True
            continue

        if escape_mode:
            if character == delimiter:
                chunk += character
            else:
                chunk += ESCAPE_CHAR + character
            escape_mode = False
        else:
            if character == delimiter:
                parts.append(chunk)
                chunk = ""
            else:
                chunk += character

    parts.append(chunk)

    return parts


def parse_parameter_values(parameter_value_string):
    """Parse a parameter value string from the CLI and return as a dict."""
    parts = _escape_split(parameter_value_string, ",")
    parameter_values = {}
    for part in parts:
        if part.strip() == "":
            continue
        try:
            name, value = _escape_split(part, "=")
        except ValueError:
            raise ValueError("Invalid parameter value: {}".format(part))
        parameter_values[name] = value
    return parameter_values
