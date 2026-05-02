"""Smoke tests for shdsl."""
import os
import sys
sys.path.insert(0, "/home/claude")

from shdsl import sh, cmd, pipe, run, CommandError

print("=" * 60)
print("Test 1: simple command, str() coercion")
print("=" * 60)
greeting = sh.echo("hello world")
print(f"  result: {greeting!r}")
assert str(greeting) == "hello world", f"got {greeting!r}"

print("\n" + "=" * 60)
print("Test 2: pipeline with |")
print("=" * 60)
result = sh.echo("apple\nbanana\ncherry") | sh.grep("an")
out = str(result)
print(f"  result: {out!r}")
assert "banana" in out, f"got {out!r}"

print("\n" + "=" * 60)
print("Test 3: three-stage pipeline")
print("=" * 60)
text = "foo\nbar\nfoo\nbaz\nfoo\n"
count = text | sh.grep("foo") | sh.wc("-l")
print(f"  count of 'foo' lines: {str(count).strip()!r}")
assert str(count).strip() == "3"

print("\n" + "=" * 60)
print("Test 4: string fed as stdin via |")
print("=" * 60)
out = "hello\nworld\n" | sh.tr("a-z", "A-Z")
print(f"  result: {str(out)!r}")
assert "HELLO" in str(out)

print("\n" + "=" * 60)
print("Test 5: redirection >")
print("=" * 60)
sh.echo("first line") > "/tmp/shdsl_test.txt"
sh.echo("second line") >> "/tmp/shdsl_test.txt"
content = sh.cat("/tmp/shdsl_test.txt")
print(f"  file contents:\n    {str(content)!r}")
assert "first line" in str(content)
assert "second line" in str(content)

print("\n" + "=" * 60)
print("Test 6: input redirection <")
print("=" * 60)
result = sh.wc("-l") < "/tmp/shdsl_test.txt"
print(f"  wc -l < file: {str(result).strip()!r}")

print("\n" + "=" * 60)
print("Test 7: iteration")
print("=" * 60)
for line in sh.echo("one\ntwo\nthree"):
    print(f"  line: {line!r}")

print("\n" + "=" * 60)
print("Test 8: .run() for full result")
print("=" * 60)
r = sh.ls("/nonexistent_path_xyz").run()
print(f"  returncode: {r.returncode}")
print(f"  stderr: {r.stderr.strip()!r}")
print(f"  ok: {r.ok}")
assert r.returncode != 0
assert not r.ok

print("\n" + "=" * 60)
print("Test 9: .checked() raises on failure")
print("=" * 60)
try:
    sh.ls("/nonexistent_path_xyz").checked().run()
    print("  ERROR: should have raised")
    sys.exit(1)
except CommandError as e:
    print(f"  correctly raised: {type(e).__name__}")

print("\n" + "=" * 60)
print("Test 10: cmd() helper for arbitrary commands")
print("=" * 60)
result = cmd("echo", "via cmd helper")
print(f"  result: {str(result)!r}")
assert "via cmd helper" in str(result)

print("\n" + "=" * 60)
print("Test 11: cmd() with shell-style string")
print("=" * 60)
result = cmd("echo hello from string")
print(f"  result: {str(result)!r}")

print("\n" + "=" * 60)
print("Test 12: pipe() helper")
print("=" * 60)
result = pipe(
    sh.echo("a\nb\nc\nd"),
    sh.grep("[bc]"),
    sh.sort("-r"),
)
print(f"  result:\n{str(result)}")

print("\n" + "=" * 60)
print("Test 13: cd context manager")
print("=" * 60)
with sh.cd("/tmp"):
    pwd = sh.pwd()
    print(f"  pwd: {str(pwd)!r}")
    assert str(pwd) == "/tmp"

print("\n" + "=" * 60)
print("Test 14: env context manager")
print("=" * 60)
with sh.env(MY_TEST_VAR="hello123"):
    val = sh.printenv("MY_TEST_VAR")
    print(f"  MY_TEST_VAR: {str(val)!r}")
    assert str(val) == "hello123"

print("\n" + "=" * 60)
print("Test 15: background execution")
print("=" * 60)
bg = sh.sleep("0.2").bg()
print(f"  bg: {bg!r}")
result = bg.wait()
print(f"  finished, rc={result.returncode}")
assert result.returncode == 0

print("\n" + "=" * 60)
print("Test 16: bool() and int() coercion")
print("=" * 60)
print(f"  bool(true): {bool(sh.true())}")
print(f"  bool(false): {bool(sh.false())}")
print(f"  int(true): {int(sh.true())}")
print(f"  int(false): {int(sh.false())}")
assert bool(sh.true()) is True
assert bool(sh.false()) is False
assert int(sh.true()) == 0
assert int(sh.false()) == 1

print("\n" + "=" * 60)
print("Test 17: with_env builder")
print("=" * 60)
result = sh.printenv("FOO").with_env(FOO="bar")
print(f"  FOO via with_env: {str(result)!r}")
assert str(result) == "bar"

# Cleanup
os.unlink("/tmp/shdsl_test.txt")

print("\n" + "=" * 60)
print("ALL TESTS PASSED")
print("=" * 60)
