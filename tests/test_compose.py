import difflib
import re
from pathlib import Path

from pydantic import BaseModel, Field

from lythonic.compose import Method
from lythonic.compose.cli import ActionTree, Main, RunContext


def my_func(x, y: bool, z=None, a: int | float = 42, b: str = "foo"):  # pyright: ignore
    pass


class Config(BaseModel):
    name: str = Field(description="The name of the server")


def zet(config: Config):
    """Set the server configuration"""
    print(config)


def test_method():
    z = Method(zet)
    assert len(z.args) == 1
    assert z.args[0].name == "config"
    assert z.args[0].annotation is Config
    assert z.args[0].default is None
    assert z.args[0].is_optional is False
    assert z.args[0].description == ""
    assert z.args[0].arg_help(0) == "<config> - Config: "

    main = Method(Main)
    assert len(main.args) == 1
    assert main.args[0].name == "help"
    assert main.args[0].annotation is bool
    assert main.args[0].default is False
    assert main.args[0].is_optional is True
    assert main.args[0].description == "Show help"
    assert main.args[0].opt_help(0) == "[--help] - bool: Show help. Default: False"

    m = Method(my_func)  # pyright: ignore
    assert len(m.args) == 5
    assert m.args[0].name == "x"
    assert m.args[0].annotation is None
    assert m.args[0].default is None
    assert m.args[0].is_optional is False
    assert m.args[0].description == ""

    assert m.args[1].name == "y"
    assert m.args[1].annotation is bool
    assert m.args[1].default is None
    assert m.args[1].is_optional is False
    assert m.args[1].description == ""

    assert m.args[2].name == "z"
    assert m.args[2].annotation is None
    assert m.args[2].default is None
    assert m.args[2].is_optional is True
    assert m.args[2].description == ""

    assert m.args[3].name == "a"
    assert str(m.args[3].annotation) == "int | float"
    assert m.args[3].default == 42
    assert m.args[3].is_optional is True
    assert m.args[3].description == ""  # "an integer or a float"

    assert m.args[4].name == "b"
    assert m.args[4].annotation is str
    assert m.args[4].default == "foo"
    assert m.args[4].is_optional is True
    assert m.args[4].description == ""


main_at = ActionTree(Main)


class Server(BaseModel):
    """Managing the server"""

    data: Path = Field(default=Path("./data"), description="directory to store all server data")


server_at = main_at.actions.add(Server)


@server_at.actions.wrap
def start(ctx: RunContext):
    """Starts the server in the foreground"""
    ctx.print(f"Starting server with data directory: {ctx.path.get('/server').data}")  # pyright: ignore[reportOptionalMemberAccess]


@server_at.actions.wrap
def stop():
    """Stops the server"""
    print("stopping server ")


@server_at.actions.wrap
def config():
    """Manage the server configuration"""
    pass


@config.actions.wrap
def set(config: Config):
    """Set the server configuration"""
    print(config)


@config.actions.wrap
def get():
    """Get the server configuration"""
    print("config get")


def test_at():
    has_ctx, args, opts = set._split_ctx_args_opts()  # pyright: ignore[reportPrivateUsage]
    assert has_ctx is False
    assert len(opts) == 0
    assert len(args) == 1


def test_cli():
    def run(cmd: str, success: bool, expected: list[str]):
        rr = main_at.run_args(cmd.split(), print_func=None)
        assert rr.success == success

        def match_output(actual: list[str], expected: list[str]) -> bool:
            if len(actual) != len(expected):
                return False
            for a, e in zip(actual, expected, strict=True):
                if e[:1] == "~":
                    if not re.compile(e[1:]).match(a.strip()):
                        return False
                elif a.strip() != e.strip():
                    return False
            return True

        match = match_output(rr.msgs, expected)
        if not match:
            print(rr.msgs)
            print("\n".join(difflib.unified_diff(rr.msgs, expected)))

        return match

    path_type = type(Path("/")).__name__
    full_help = [
        "Usage: ",
        "  wgl ",
        "    [--help] - bool: Show help. Default: False",
        "    Actions:",
        "      server - Managing the server",
        f"          [--data=value] - Path: directory to store all server data. Default: {path_type}('data')",
        "          Actions:",
        "            start - Starts the server in the foreground",
        "            stop - Stops the server",
        "            config - Manage the server configuration",
        "                Actions:",
        "                  set - Set the server configuration",
        "                      <config> - Config: ",
        "                  get - Get the server configuration",
    ]
    assert run("wgl -help", False, ["Error: Argument '-help' is not a valid", *full_help])
    assert run(
        "wgl --help",
        False,
        ["Error: Action need to be specified, expected one of server", *full_help],
    )
    assert run(
        "wgl --help=y",
        False,
        ["Error: Action need to be specified, expected one of server", *full_help],
    )
    assert run("wgl --help=y --help", False, ["Error: --help is already set to True", *full_help])
    assert run(
        "wgl server",
        False,
        [
            "Error: Action need to be specified, expected one of start, stop, config",
            "Usage: ",
            "  wgl server",
            *full_help[5:],
        ],
    )
    assert run("wgl server start", True, ["Starting server with data directory: data"])
    assert run("wgl server stop", True, [])
    assert run(
        "wgl server config",
        False,
        [
            "Error: Action need to be specified, expected one of set, get",
            "Usage: ",
            "  wgl server config",
            *full_help[10:],
        ],
    )
    assert run(
        "wgl server config set",
        False,
        [
            "Error: Required arguments are missing: <config>",
            "Usage: ",
            "  wgl server config set",
            "    <config> - Config: ",
        ],
    )
    assert run("wgl server config get", True, [])
    assert run(
        "wgl server config set {}",
        False,
        [
            r"~Error: 1 validation error for Config\s+name\s+Field required .*",
            "Usage: ",
            "  wgl server config set",
            "    <config> - Config: ",
        ],
    )
    assert run('wgl server config set {"name":"test"}', True, [])
