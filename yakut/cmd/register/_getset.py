# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
from typing import Sequence, TYPE_CHECKING, Union, Any
import pycyphal
import yakut
from ._common import Result, ProgressCallback
from yakut.register import value_as_simplified_builtin

if TYPE_CHECKING:
    import pycyphal.application
    from uavcan.register import Access_1


async def getset(
    local_node: "pycyphal.application.Node",
    progress: ProgressCallback,
    node_ids: Sequence[int],
    *,
    reg_name: str,
    reg_val_str: str | None,
    maybe_no_service: bool,
    maybe_missing: bool,
    timeout: float,
    asis: bool,
) -> Result:
    res = Result()
    for nid, sample in (await _getset(local_node, progress, node_ids, reg_name, reg_val_str, timeout=timeout)).items():
        _logger.debug("Register @%r: %r", nid, sample)
        res.data_per_node[nid] = None  # Error state is default state
        if isinstance(sample, _NoService):
            if maybe_no_service:
                res.warnings.append(f"Service not accessible at node {nid}, ignoring as requested")
            else:
                res.errors.append(f"Service not accessible at node {nid}")

        elif isinstance(sample, _Timeout):
            res.errors.append(f"Request to node {nid} has timed out")

        elif isinstance(sample, Exception):
            res.errors.append(f"Assignment failed on node {nid}: {type(sample).__name__}: {sample}")

        else:
            if sample.value.empty and reg_val_str is not None:
                if maybe_missing:
                    res.warnings.append(f"Nonexistent register {reg_name!r} at node {nid} ignored as requested")
                else:
                    res.errors.append(f"Cannot assign nonexistent register {reg_name!r} at node {nid}")
            res.data_per_node[nid] = _represent(sample, asis=asis)
    return res


class _NoService:
    pass


class _Timeout:
    pass


async def _getset(
    local_node: pycyphal.application.Node,
    progress: ProgressCallback,
    node_ids: Sequence[int],
    reg_name: str,
    reg_val_str: str | None,
    *,
    timeout: float,
) -> dict[int, Union[_NoService, _Timeout, "Access_1.Response", "pycyphal.application.register.ValueConversionError"],]:
    from uavcan.register import Access_1

    out: dict[
        int,
        Access_1.Response | _NoService | _Timeout | pycyphal.application.register.ValueConversionError,
    ] = {}
    for nid in node_ids:
        progress(f"{reg_name!r}@{nid:05}")
        cln = local_node.make_client(Access_1, nid)
        try:
            cln.response_timeout = timeout
            out[nid] = await _getset_one(cln, reg_name, reg_val_str)
        finally:
            cln.close()
    progress("Done")
    return out


async def _getset_one(
    client: pycyphal.presentation.Client["Access_1"],
    reg_name: str,
    reg_val_str: str | None,
) -> Union[_NoService, _Timeout, "Access_1.Response", "pycyphal.application.register.ValueConversionError"]:
    from uavcan.register import Access_1, Name_1
    from pycyphal.application.register import ValueProxy, ValueConversionError

    resp = await client(Access_1.Request(name=Name_1(reg_name)))
    if resp is None:
        return _NoService()
    assert isinstance(resp, Access_1.Response)
    if reg_val_str is None or resp.value.empty:  # Modification is not required or there is no such register.
        return resp

    # Coerce the supplied value to the type of the remote register.
    assert not resp.value.empty
    val = ValueProxy(resp.value)
    try:
        val.assign_environment_variable(reg_val_str)
    except ValueConversionError as ex:  # Oops, not coercible (e.g., register is float[], value is string)
        return ex

    # Write the coerced value to the node; it may also modify it so return the response, not the coercion result.
    resp = await client(Access_1.Request(name=Name_1(reg_name), value=val.value))
    if resp is None:  # We got a response before but now we didn't, something is messed up so the result is different.
        return _Timeout()
    assert isinstance(resp, Access_1.Response)
    return resp


def _represent(response: "Access_1.Response", *, asis: bool) -> Any:
    if asis:
        return pycyphal.dsdl.to_builtin(response)
    return value_as_simplified_builtin(response.value)


_logger = yakut.get_logger(__name__)
