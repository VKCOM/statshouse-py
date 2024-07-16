import subprocess

def test_import():
    from statshouse import (
        DEFAULT_STATSHOUSE_ADDR,
        TAG_STRING_TOP,
        TAG_HOST,
        Client,
        count,
        value,
        unique,
    )

def test_integration():
    subprocess.run(["go", "run", "github.com/vkcom/statshouse/cmd/statshouse-client-test@master"], check=True)
