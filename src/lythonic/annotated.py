import inspect
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, Generic, NamedTuple, TypeVar

from pydantic import BaseModel, Field
from typing_extensions import override

from lythonic import GlobalRef


class ArgInfo(NamedTuple):
    name: str
    annotation: Any | None
    default: Any | None
    is_optional: bool
    description: str

    @classmethod
    def from_param(cls, param: inspect.Parameter, origin: Any):
        description = ""
        is_optional = param.default != inspect.Parameter.empty
        default = param.default if is_optional else None
        if isinstance(origin, type) and issubclass(origin, BaseModel):
            if param.name in origin.model_fields:
                field = origin.model_fields[param.name]
                if field.description is not None:
                    description = field.description
                is_optional = not field.is_required()
                default = field.default
        return cls(
            name=param.name,
            annotation=param.annotation if param.annotation != inspect.Parameter.empty else None,
            default=default,
            is_optional=is_optional,
            description=description,
        )

    def to_value(self, v: str):
        if self.annotation is None:
            return v
        if self.annotation is bool:
            return v.lower() in ("true", "1", "yes", "y")
        if issubclass(self.annotation, BaseModel):
            return self.annotation.model_validate_json(v)
        return self.annotation(v)

    def is_turn_on_option(self) -> bool:
        return self.annotation is bool and self.default is False

    @property
    def type(self) -> str:
        if self.annotation is not None:
            return self.annotation.__name__
        return "str"

    def arg_help(self, indent: int):
        return f"{' ' * indent}<{self.name}> - {self.type}: {self.description}"

    def opt_help(self, indent: int):
        return f"{' ' * indent}[--{self.name}{'=value' if not self.is_turn_on_option() else ''}] - {self.type}: {self.description}. Default: {self.default!r}"


class Method:
    gref: GlobalRef
    _o: Callable[..., Any] | None
    _args: list[ArgInfo] | None
    _args_by_name: dict[str, ArgInfo] | None
    _return_annotation: Any | None

    def __init__(self, o: Callable[..., Any] | GlobalRef):
        if isinstance(o, GlobalRef):
            self.gref = o
            self._o = None
        else:
            self.gref = GlobalRef(o)
            assert isinstance(o, Callable), "method instance must be a callable"
            self._o = o
        self._args = None
        self._args_by_name = None
        self._return_annotation = None

    def _update_from_signature(self):
        o = self.o
        sig = inspect.signature(o)
        self._args = [ArgInfo.from_param(param, origin=o) for param in sig.parameters.values()]
        self._args_by_name = {arg.name: arg for arg in self._args}
        self._return_annotation = sig.return_annotation

    @property
    def o(self) -> Callable[..., Any]:
        if self._o is None:
            self._o = self.gref.get_instance()
        assert self._o is not None
        return self._o

    @property
    def args(self) -> list[ArgInfo]:
        if self._args is None:
            self._update_from_signature()
        assert self._args is not None
        return self._args

    @property
    def args_by_name(self) -> dict[str, ArgInfo]:
        if self._args_by_name is None:
            self._update_from_signature()
        assert self._args_by_name is not None
        return self._args_by_name

    @property
    def return_annotation(self) -> Any | None:
        if self._args is None:
            self._update_from_signature()
        return self._return_annotation

    @property
    def name(self):
        return self.gref.name

    @property
    def doc(self):
        return self.o.__doc__

    def __call__(self, *args: Any, **kwargs: Any):
        return self.o(*args, **kwargs)


T = TypeVar("T", bound=Method)


class MethodDict(Generic[T], dict[str, T]):
    method_type: type[T]

    def __init__(self, method_type: type[T]):
        super().__init__()
        self.method_type = method_type

    def add(self, o: Callable[..., Any]) -> T:
        m = self.method_type(o)
        self[m.name.lower()] = m
        return m

    def wrap(self, o: Callable[..., Any]) -> T:
        return self.add(o)


class RunResult:
    success: bool
    msgs: list[str]
    print_func: Callable[[str], None] | None

    def __init__(self, success: bool = False, print_func: Callable[[str], None] | None = print):
        self.success = success
        self.msgs = []
        self.print_func = print_func

    def print(self, msg: str):
        if self.print_func is None:
            self.msgs.append(msg)
        else:
            self.print_func(msg)


class ActionTree(Method):
    actions: MethodDict["ActionTree"]

    def __init__(self, o: Callable[..., Any] | GlobalRef):
        super().__init__(o)
        self.actions = MethodDict["ActionTree"](method_type=self.__class__)

    def run_args(
        self, argv: list[str], print_func: Callable[[str], None] | None = print
    ) -> RunResult:
        cli_name = Path(argv[0]).name
        cli_args = argv[1:]
        return self._run_args(cli_args, RunContext(self, cli_name, print_func))

    def _split_ctx_args_opts(self) -> tuple[bool, list[ArgInfo], list[ArgInfo]]:
        has_ctx: bool = (
            len(self.args) > 0
            and self.args[0].name == "ctx"
            and self.args[0].annotation == RunContext
        )
        self_args = self.args[1:] if has_ctx else self.args
        return (
            has_ctx,
            [arg for arg in self_args if not arg.is_optional],
            [arg for arg in self_args if arg.is_optional],
        )

    def _run_args(self, cli_args: list[str], ctx: "RunContext") -> RunResult:
        current_arg_index = 0
        arg_values = {}
        has_ctx, required_args, _ = self._split_ctx_args_opts()
        if has_ctx:
            arg_values["ctx"] = ctx
        error = None
        try:
            while current_arg_index < len(cli_args):
                arg_str = cli_args[current_arg_index]
                if required_args:
                    arg = required_args[0]
                    if arg_str.startswith("--"):
                        raise ValueError(
                            f"Argument {arg.name} is required, but getting option {arg_str}"
                        )
                    current_arg_index += 1
                    arg_values[arg.name] = arg.to_value(arg_str)
                    required_args.pop(0)
                    continue
                if arg_str.startswith("--"):
                    arg_str = arg_str[2:]
                    if "=" in arg_str:
                        k, v = arg_str.split("=", 1)
                    else:
                        k = arg_str
                        v = None
                    if k in arg_values:
                        raise ValueError(f"--{k} is already set to {arg_values[k]}")
                    if k not in self.args_by_name:
                        raise ValueError(
                            f"--{k} is not a valid option, expected one of {list(map(lambda x: f'--{x}', self.args_by_name.keys()))}"
                        )
                    arg = self.args_by_name[k]
                    if v is None:
                        if arg.is_turn_on_option():
                            v = "y"
                        else:
                            current_arg_index += 1
                            if current_arg_index >= len(cli_args):
                                raise ValueError(f"Value is not provided for --{k}")
                            v = cli_args[current_arg_index]
                    arg_values[k] = arg.to_value(v)
                    current_arg_index += 1
                    continue
                elif arg_str in self.actions:
                    action: ActionTree = self.actions[arg_str]
                    ctx.path.value = self(**arg_values)
                    ctx.add_path(arg_str)
                    return action._run_args(cli_args[current_arg_index + 1 :], ctx)
                raise ValueError(f"Argument {arg_str!r} is not a valid")
            if required_args:
                raise ValueError(
                    f"Required arguments are missing: {' '.join(f'<{arg.name}>' for arg in required_args)}"
                )
            if self.actions:
                raise ValueError(
                    f"Action need to be specified, expected one of {', '.join(self.actions.keys())}"
                )
        except Exception:
            error = str(sys.exc_info()[1])
        if error or ctx.is_print_help_selected():
            ctx.print_help(error, ctx.path, self)
        else:
            ctx.path.value = self(**arg_values)
            ctx.run_result.success = True
        return ctx.run_result


class Main(BaseModel):
    help: bool = Field(default=False, description="Show help")


class PathValue:
    """
    Records values along a path.

    >>> root = PathValue.root()
    >>> root.value = "I am Groot"
    >>> foo = PathValue("foo", root, "I am foo")
    >>> foo.get("/foo")
    'I am foo'
    >>> foo.parts
    ('', 'foo')
    >>> root.parts
    ('',)
    >>> foo.get("/bar")
    Traceback (most recent call last):
    ...
    ValueError: Path /bar is not contained in /foo
    >>> foo.get("/foo/")
    'I am foo'
    >>> foo.get("/")
    'I am Groot'


    """

    name: str
    parent: "PathValue | None"
    value: Any | None

    def __init__(self, name: str, parent: "PathValue | None", value: Any | None = None) -> None:
        if name == "":
            assert parent is None
        else:
            assert parent is not None
        self.name = name
        self.parent = parent
        self.value = value

    @classmethod
    def root(cls) -> "PathValue":
        return cls(name="", parent=None)

    @property
    def parts(self) -> tuple[str, ...]:
        if self.parent is None:
            return (self.name,)
        return (*self.parent.parts, self.name)

    def get_back(self, n: int) -> Any | None:
        if n == 0:
            return self.value
        if self.parent is None:
            return None
        return self.parent.get_back(n - 1)

    def get(self, path: str) -> Any | None:
        parts = self.parts
        lookup_parts = tuple(path.split("/"))
        if lookup_parts[-1] == "":
            lookup_parts = lookup_parts[:-1]
        lpl = len(lookup_parts)
        pl = len(parts)
        if parts[:lpl] != lookup_parts:
            raise ValueError(f"Path {path} is not contained in {self}")
        return self.get_back(pl - lpl)

    @override
    def __str__(self):
        return "/".join(self.parts)


class RunContext:
    path: PathValue
    main_at: ActionTree
    run_result: RunResult
    cli_name: str

    def __init__(
        self, main_at: ActionTree, cli_name: str, print_func: Callable[[str], None] | None = print
    ):
        self.path = PathValue.root()
        self.main_at = main_at
        self.run_result = RunResult(print_func=print_func)
        self.cli_name = cli_name

    def add_path(self, path: str):
        self.path = PathValue(path, self.path)

    def is_print_help_selected(self) -> bool:
        v = self.path.get("/")
        if isinstance(v, Main):
            return v.help
        return False

    def print(self, msg: str):
        self.run_result.print(msg)

    def _recursive_help(self, print_at: ActionTree, indent: int):
        indent += 2
        _, arguments, options = print_at._split_ctx_args_opts()  # pyright: ignore[reportPrivateUsage]
        for arg in arguments:
            self.print(arg.arg_help(indent))
        for opt in options:
            self.print(opt.opt_help(indent))
        if print_at.actions:
            self.print(f"{' ' * indent}Actions:")
            indent += 2
            for action_name, action in print_at.actions.items():
                self.print(f"{' ' * indent}{action_name} - {action.doc}")
                self._recursive_help(action, indent + 2)

    def print_help(self, error: str | None, path: PathValue, current_at: ActionTree):
        if error:
            self.print(f"Error: {error}")
        self.print("Usage: ")
        self.print(f"  {self.cli_name} {' '.join(path.parts[1:])}")
        self._recursive_help(current_at, 2)
