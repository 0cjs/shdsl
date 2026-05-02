"""
shdsl - a shell-like embedded DSL for Python.

Inspired by pwntools' ergonomics. The goal is to make shell pipelines feel
natural in Python without sacrificing introspection or composability.

Quick examples
--------------

    from shdsl import sh, cmd, pipe

    # attribute-style commands
    print(sh.echo("hello world"))
    print(sh.ls("-la", "/tmp"))

    # pipelines with `|`
    result = sh.cat("/etc/passwd") | sh.grep("root") | sh.wc("-l")
    print(result)

    # redirection with `>` and `>>`
    sh.echo("line one") > "/tmp/out.txt"
    sh.echo("line two") >> "/tmp/out.txt"

    # input redirection with `<`
    count = sh.wc("-l") < "/tmp/out.txt"

    # use as iterator (line by line)
    for line in sh.cat("/etc/hosts"):
        print(line)

    # capture exit code, stderr, stdout separately
    r = sh.ls("/nope").run()
    print(r.returncode, r.stderr)

    # background execution
    bg = sh.sleep("5").bg()
    bg.wait()

    # arbitrary commands via cmd()
    cmd("git", "status") | cmd("head", "-n", "5")

    # context manager for cwd / env
    with sh.cd("/tmp"):
        print(sh.pwd())
"""

from __future__ import annotations

import io
import os
import shlex
import signal
import subprocess
import sys
import threading
from contextlib import contextmanager
from typing import IO, Iterable, Iterator, List, Optional, Sequence, Union


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

class CommandResult(str):
    """
    A subclass of str so the common case (`print(sh.echo("hi"))`) "just works",
    but it also carries returncode/stderr metadata.
    """

    __slots__ = ("returncode", "stderr", "argv")

    def __new__(cls, stdout: str, returncode: int, stderr: str, argv: Sequence[str]):
        instance = super().__new__(cls, stdout)
        instance.returncode = returncode
        instance.stderr = stderr
        instance.argv = list(argv)
        return instance

    @property
    def stdout(self) -> str:
        return str(self)

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def __repr__(self) -> str:
        return (
            f"CommandResult(argv={self.argv!r}, returncode={self.returncode}, "
            f"stdout={str(self)!r}, stderr={self.stderr!r})"
        )


class CommandError(RuntimeError):
    def __init__(self, result: CommandResult):
        self.result = result
        super().__init__(
            f"command failed (exit {result.returncode}): "
            f"{' '.join(shlex.quote(a) for a in result.argv)}\n{result.stderr}"
        )


# ---------------------------------------------------------------------------
# The core: a lazy Command object that supports |, >, <, >>, iteration, etc.
# ---------------------------------------------------------------------------

class Command:
    """
    Represents a (possibly piped) command. Lazy: nothing runs until you
    materialize it via str(), iteration, .run(), redirection, or printing.
    """

    def __init__(
        self,
        argv: Sequence[str],
        *,
        cwd: Optional[str] = None,
        env: Optional[dict] = None,
        check: bool = False,
        stdin_data: Optional[Union[str, bytes]] = None,
        upstream: Optional["Command"] = None,
        text: bool = True,
    ):
        self.argv: List[str] = [str(a) for a in argv]
        self.cwd = cwd
        self.env = env
        self.check = check
        self.stdin_data = stdin_data
        self.upstream = upstream  # the previous command in a pipeline, if any
        self.text = text

    # -- piping -------------------------------------------------------------

    def __or__(self, other: "Command") -> "Command":
        """`self | other` — pipe self's stdout into other's stdin."""
        if not isinstance(other, Command):
            raise TypeError(f"cannot pipe Command into {type(other).__name__}")
        # Clone other and chain it onto self
        chained = Command(
            other.argv,
            cwd=other.cwd,
            env=other.env,
            check=other.check,
            stdin_data=other.stdin_data,
            upstream=self,
            text=other.text,
        )
        return chained

    def __ror__(self, other) -> "Command":
        """Allow `"some string" | sh.grep("x")` — feed a string as stdin."""
        if isinstance(other, str) or isinstance(other, bytes):
            return Command(
                self.argv,
                cwd=self.cwd,
                env=self.env,
                check=self.check,
                stdin_data=other,
                upstream=None,
                text=self.text,
            )
        if hasattr(other, "read"):
            data = other.read()
            return Command(
                self.argv,
                cwd=self.cwd,
                env=self.env,
                check=self.check,
                stdin_data=data,
                upstream=None,
                text=self.text,
            )
        return NotImplemented

    # -- redirection --------------------------------------------------------

    def __gt__(self, path) -> CommandResult:
        """`cmd > "file"` — write stdout to file (truncate)."""
        return self._redirect_out(path, mode="w")

    def __rshift__(self, path) -> CommandResult:
        """`cmd >> "file"` — append stdout to file."""
        return self._redirect_out(path, mode="a")

    def __lt__(self, path) -> CommandResult:
        """`cmd < "file"` — feed file contents as stdin."""
        with open(path, "rb") as f:
            data = f.read()
        new = Command(
            self.argv,
            cwd=self.cwd,
            env=self.env,
            check=self.check,
            stdin_data=data,
            upstream=None,
            text=self.text,
        )
        return new.run()

    def _redirect_out(self, path, mode: str) -> CommandResult:
        result = self.run()
        with open(path, mode) as f:
            f.write(result.stdout)
        return result

    # -- materialization ----------------------------------------------------

    def run(self) -> CommandResult:
        """Execute the (chain of) command(s) and return a CommandResult."""
        return _execute_pipeline(self)

    def bg(self) -> "BackgroundProcess":
        """Run in the background. Returns a BackgroundProcess handle."""
        return BackgroundProcess(self)

    def __str__(self) -> str:
        # Strip a single trailing newline for ergonomic interpolation
        out = self.run()
        if out.endswith("\n"):
            return out[:-1]
        return str.__str__(out)

    def __repr__(self) -> str:
        chain = []
        cur: Optional[Command] = self
        while cur is not None:
            chain.append(" ".join(shlex.quote(a) for a in cur.argv))
            cur = cur.upstream
        chain.reverse()
        return f"<Command: {' | '.join(chain)}>"

    def __iter__(self) -> Iterator[str]:
        """Iterate over output lines (without trailing newlines)."""
        result = self.run()
        text = result.stdout
        if not text:
            return iter([])
        # Remove a single trailing newline so we don't yield an empty final line
        if text.endswith("\n"):
            text = text[:-1]
        return iter(text.split("\n"))

    def __bool__(self) -> bool:
        return self.run().ok

    def __int__(self) -> int:
        return self.run().returncode

    def lines(self) -> List[str]:
        return list(self)

    # -- builder-style modifiers (return a new Command) --------------------

    def with_cwd(self, cwd: str) -> "Command":
        return Command(
            self.argv,
            cwd=cwd,
            env=self.env,
            check=self.check,
            stdin_data=self.stdin_data,
            upstream=self.upstream,
            text=self.text,
        )

    def with_env(self, **env) -> "Command":
        merged = dict(self.env or os.environ)
        merged.update({k: str(v) for k, v in env.items()})
        return Command(
            self.argv,
            cwd=self.cwd,
            env=merged,
            check=self.check,
            stdin_data=self.stdin_data,
            upstream=self.upstream,
            text=self.text,
        )

    def checked(self) -> "Command":
        """Return a copy that raises CommandError on non-zero exit."""
        return Command(
            self.argv,
            cwd=self.cwd,
            env=self.env,
            check=True,
            stdin_data=self.stdin_data,
            upstream=self.upstream,
            text=self.text,
        )


# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------

def _execute_pipeline(tail: Command) -> CommandResult:
    """
    Execute the chain ending at `tail`. Walks .upstream backward to assemble
    the full pipeline, then runs all stages concurrently with real OS pipes.
    """
    # Collect from head to tail
    stages: List[Command] = []
    cur: Optional[Command] = tail
    while cur is not None:
        stages.append(cur)
        cur = cur.upstream
    stages.reverse()

    # The first stage may have stdin_data; subsequent stages get piped input
    procs: List[subprocess.Popen] = []
    prev_stdout: Optional[Union[int, IO]] = None

    head = stages[0]
    head_input: Optional[bytes] = None
    if head.stdin_data is not None:
        if isinstance(head.stdin_data, str):
            head_input = head.stdin_data.encode()
        else:
            head_input = head.stdin_data

    try:
        for i, stage in enumerate(stages):
            is_last = i == len(stages) - 1
            stdin_arg = (
                subprocess.PIPE if i == 0 and head_input is not None
                else prev_stdout if i > 0
                else None
            )
            stdout_arg = subprocess.PIPE  # always capture so we can return it
            stderr_arg = subprocess.PIPE

            try:
                p = subprocess.Popen(
                    stage.argv,
                    stdin=stdin_arg,
                    stdout=stdout_arg,
                    stderr=stderr_arg,
                    cwd=stage.cwd,
                    env=stage.env,
                )
            except FileNotFoundError as e:
                # Clean up any started procs
                for pp in procs:
                    pp.kill()
                raise CommandError(
                    CommandResult("", 127, str(e), stage.argv)
                ) from None

            procs.append(p)

            # Close the previous stdout in the parent so the child gets EOF
            if i > 0 and prev_stdout is not None and hasattr(prev_stdout, "close"):
                prev_stdout.close()

            prev_stdout = p.stdout

        # Feed input to the head process if applicable
        if head_input is not None:
            head_proc = procs[0]

            def _feed(p, data):
                try:
                    p.stdin.write(data)
                    p.stdin.close()
                except BrokenPipeError:
                    pass

            t = threading.Thread(target=_feed, args=(head_proc, head_input), daemon=True)
            t.start()

        # Read final stdout and all stderrs
        last = procs[-1]
        out_bytes = last.stdout.read() if last.stdout else b""
        last.stdout and last.stdout.close()

        # Collect stderr from all stages (concat)
        stderr_chunks: List[bytes] = []
        for p in procs:
            if p.stderr:
                stderr_chunks.append(p.stderr.read())
                p.stderr.close()

        # Wait for everyone
        for p in procs:
            p.wait()

        stdout_text = out_bytes.decode(errors="replace") if tail.text else out_bytes  # type: ignore
        stderr_text = b"".join(stderr_chunks).decode(errors="replace")
        rc = procs[-1].returncode

        result = CommandResult(stdout_text if isinstance(stdout_text, str) else "",
                               rc, stderr_text, tail.argv)

        if tail.check and rc != 0:
            raise CommandError(result)

        return result

    except Exception:
        for p in procs:
            try:
                p.kill()
            except Exception:
                pass
        raise


# ---------------------------------------------------------------------------
# Background processes
# ---------------------------------------------------------------------------

class BackgroundProcess:
    def __init__(self, command: Command):
        self._command = command
        # For simplicity, background only supports single-stage commands.
        # Pipelines could be supported but add complexity around lifetime.
        if command.upstream is not None:
            raise ValueError("bg() does not support pipelines yet; use .run() in a thread")
        stdin_arg = subprocess.PIPE if command.stdin_data is not None else None
        self._proc = subprocess.Popen(
            command.argv,
            stdin=stdin_arg,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=command.cwd,
            env=command.env,
        )
        if command.stdin_data is not None:
            data = command.stdin_data
            if isinstance(data, str):
                data = data.encode()
            try:
                self._proc.stdin.write(data)
                self._proc.stdin.close()
            except BrokenPipeError:
                pass

    @property
    def pid(self) -> int:
        return self._proc.pid

    def wait(self, timeout: Optional[float] = None) -> CommandResult:
        try:
            out, err = self._proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise
        return CommandResult(
            out.decode(errors="replace") if out else "",
            self._proc.returncode,
            err.decode(errors="replace") if err else "",
            self._command.argv,
        )

    def kill(self, sig: int = signal.SIGTERM):
        try:
            self._proc.send_signal(sig)
        except ProcessLookupError:
            pass

    def poll(self) -> Optional[int]:
        return self._proc.poll()

    def __repr__(self) -> str:
        return f"<BackgroundProcess pid={self.pid} argv={self._command.argv!r}>"


# ---------------------------------------------------------------------------
# The `sh` namespace: attribute access becomes a command
# ---------------------------------------------------------------------------

class _Shell:
    """
    Attribute access on this object yields a callable that builds Commands.

    Special members:
        sh.cd(path)   -> a context manager that chdir's
        sh.env(**)    -> a context manager that mutates os.environ
    """

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        # Translate underscores to dashes for things like sh.git_status?
        # No — that's lossy. Users should use cmd("git", "status") for multi-word.
        return _CommandBuilder(name)

    @staticmethod
    @contextmanager
    def cd(path: str):
        old = os.getcwd()
        os.chdir(path)
        try:
            yield
        finally:
            os.chdir(old)

    @staticmethod
    @contextmanager
    def env(**kwargs):
        old = {}
        try:
            for k, v in kwargs.items():
                old[k] = os.environ.get(k)
                os.environ[k] = str(v)
            yield
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


class _CommandBuilder:
    """Returned by sh.<name>; calling it produces a Command."""

    def __init__(self, name: str):
        self._name = name

    def __call__(self, *args, **kwargs) -> Command:
        argv = [self._name, *[str(a) for a in args]]
        return Command(argv, **kwargs)

    def __repr__(self) -> str:
        return f"<command:{self._name}>"


# ---------------------------------------------------------------------------
# Top-level helpers
# ---------------------------------------------------------------------------

sh = _Shell()


def cmd(*args, **kwargs) -> Command:
    """Build a Command from an argv list. Useful when the program name has
    characters that aren't valid Python identifiers, or for multi-word names."""
    if len(args) == 1 and isinstance(args[0], str) and " " in args[0]:
        argv = shlex.split(args[0])
    else:
        argv = [str(a) for a in args]
    return Command(argv, **kwargs)


def pipe(*commands: Command) -> Command:
    """Chain a list of commands into a pipeline. Equivalent to a | b | c."""
    if not commands:
        raise ValueError("pipe() needs at least one command")
    result = commands[0]
    for c in commands[1:]:
        result = result | c
    return result


def run(argv, **kwargs) -> CommandResult:
    """Convenience: run a command list immediately."""
    if isinstance(argv, str):
        argv = shlex.split(argv)
    return Command(list(argv), **kwargs).run()


__all__ = [
    "sh",
    "cmd",
    "pipe",
    "run",
    "Command",
    "CommandResult",
    "CommandError",
    "BackgroundProcess",
]
