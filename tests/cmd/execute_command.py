# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import asyncio
from typing import Any, AsyncIterable, Callable, Awaitable
import json
import concurrent.futures
import pytest
import pycyphal
from tests.dsdl import OUTPUT_DIR
from tests.transport import TransportFactory
from tests.subprocess import execute_cli
from yakut.util import EXIT_CODE_UNSUCCESSFUL


class Remote:
    def __init__(self, name: str, env: dict[str, str]) -> None:
        from pycyphal.application import make_registry, make_node, NodeInfo
        from uavcan.node import ExecuteCommand_1

        self._node = make_node(
            NodeInfo(name=name),
            make_registry(environment_variables=env),
        )
        self.last_request: ExecuteCommand_1.Request | None = None
        self.next_response: ExecuteCommand_1.Response | None = None

        async def serve_execute_command(
            req: ExecuteCommand_1.Request,
            _meta: pycyphal.presentation.ServiceRequestMetadata,
        ) -> ExecuteCommand_1.Response | None:
            # print(self._node, req, _meta, self.next_response, sep="\n\t")
            self.last_request = req
            return self.next_response

        self._srv = self._node.get_server(ExecuteCommand_1)
        self._srv.serve_in_background(serve_execute_command)
        self._node.start()

    def close(self) -> None:
        self._srv.close()
        self._node.close()


Runner = Callable[..., Awaitable[Any]]


@pytest.fixture
async def _context(
    compiled_dsdl: Any,
    transport_factory: TransportFactory,
) -> AsyncIterable[tuple[Runner, tuple[Remote, Remote]]]:
    asyncio.get_running_loop().slow_callback_duration = 10.0
    _ = compiled_dsdl
    remote_nodes = (
        Remote(f"remote_10", env=transport_factory(10).environment),
        Remote(f"remote_11", env=transport_factory(11).environment),
    )
    background_executor = concurrent.futures.ThreadPoolExecutor()

    async def run(*args: str) -> tuple[int, Any]:
        def call() -> tuple[int, Any]:
            status, stdout, _stderr = execute_cli(
                "cmd",
                *args,
                environment_variables={
                    **transport_factory(100).environment,
                    "YAKUT_PATH": str(OUTPUT_DIR),
                },
                timeout=10,
                ensure_success=False,
            )
            return status, json.loads(stdout) if stdout else None

        return await asyncio.get_running_loop().run_in_executor(background_executor, call)

    yield run, remote_nodes
    for rn in remote_nodes:
        rn.close()
    await asyncio.sleep(1.0)


@pytest.mark.asyncio
async def _unittest_basic(_context: tuple[Runner, tuple[Remote, Remote]]) -> None:
    from uavcan.node import ExecuteCommand_1

    run, (remote_10, remote_11) = _context

    # SUCCESS
    remote_10.next_response = ExecuteCommand_1.Response(status=0)
    remote_11.next_response = ExecuteCommand_1.Response(status=0)
    assert await run("10-12", "restart", "--timeout=3") == (
        0,
        {
            "10": {"output": '', "status": 0},
            "11": {"output": '', "status": 0},
        },
    )
    assert await run("10-12", "111", "COMMAND ARGUMENT", "--timeout=3") == (
        0,
        {
            "10": {"output": '', "status": 0},
            "11": {"output": '', "status": 0},
        },
    )
    assert (
        remote_10.last_request
        and remote_10.last_request.command == 111
        and remote_10.last_request.parameter.tobytes().decode() == "COMMAND ARGUMENT"
    )
    assert (
        remote_11.last_request
        and remote_11.last_request.command == 111
        and remote_11.last_request.parameter.tobytes().decode() == "COMMAND ARGUMENT"
    )

    # REMOTE ERROR; PROPAGATED AND IGNORED
    remote_10.next_response = ExecuteCommand_1.Response(status=100)
    remote_11.next_response = ExecuteCommand_1.Response(status=200)
    assert await run("10-12", "restart", "--timeout=3") == (
        EXIT_CODE_UNSUCCESSFUL,
        {
            "10": {"output": '', "status": 100},
            "11": {"output": '', "status": 200},
        },
    )
    assert await run("10-12", "123", "--expect=100,200", "--timeout=3") == (
        0,
        {
            "10": {"output": '', "status": 100},
            "11": {"output": '', "status": 200},
        },
    )
    assert remote_10.last_request and remote_10.last_request.command == 123
    assert remote_11.last_request and remote_11.last_request.command == 123

    # ONE TIMED OUT; ERROR PROPAGATED AND IGNORED
    remote_10.next_response = None
    remote_11.next_response = ExecuteCommand_1.Response(status=0)
    assert await run("10-12", "123", "--timeout=3") == (
        EXIT_CODE_UNSUCCESSFUL,
        {
            "10": None,
            "11": {"output": '', "status": 0},
        },
    )
    assert await run("10-12", "123", "--expect") == (
        0,
        {
            "10": None,
            "11": {"output": '', "status": 0},
        },
    )

    # FLAT OUTPUT (NOT GROUPED BY NODE-ID)
    remote_11.next_response = ExecuteCommand_1.Response(status=210)
    assert await run("11", "123", "FOO BAR", "--timeout=3") == (
        EXIT_CODE_UNSUCCESSFUL,
        {"output": '', "status": 210},
    )
    assert (
        remote_11.last_request
        and remote_11.last_request.command == 123
        and remote_11.last_request.parameter.tobytes().decode() == "FOO BAR"
    )
    assert await run("11", "222", "--timeout=3", "--expect=0..256") == (
        0,
        {"output": '', "status": 210},
    )
    assert (
        remote_11.last_request
        and remote_11.last_request.command == 222
        and remote_11.last_request.parameter.tobytes().decode() == ""
    )

    # ERRORS
    assert (await run("bad"))[0] != 0
    assert (await run("10", "invalid_command"))[0] != 0
    assert (await run("10", "99999999999"))[0] != 0  # Bad command code, serialization will fail
    assert (await run("10", "0", "z" * 1024))[0] != 0  # Bad parameter, serialization will fail
