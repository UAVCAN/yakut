# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import sys
from typing import Any, Sequence
import logging
import contextlib
import click
import pycyphal
from pycyphal.presentation import Presentation, Subscriber
import yakut
from yakut.param.formatter import Formatter
from yakut.util import convert_transfer_metadata_to_builtin
from yakut import dtype_loader


_logger = yakut.get_logger(__name__)


@yakut.subcommand()
@click.argument("subject", type=str, nargs=-1)
@click.option(
    "--with-metadata/--no-metadata",
    "+M/-M",
    default=False,
    show_default=True,
    help="When enabled, each message object is prepended with an extra field named `_metadata_`.",
)
@click.option(
    "--count",
    "-N",
    type=int,
    metavar="CARDINAL",
    help=f"""
Exit automatically after this many messages (or synchronous message groups) have been received. No limit by default.
""",
)
@yakut.pass_purser
@yakut.asynchronous
async def subscribe(
    purser: yakut.Purser,
    subject: tuple[str, ...],
    with_metadata: bool,
    count: int | None,
) -> None:
    """
    Subscribe to specified subjects and print messages into stdout.
    This command does not instantiate a local node and does not disturb the network in any way,
    so many instances can be cheaply executed concurrently.
    It is recommended to use anonymous transport (i.e., without a node-ID).

    The arguments are a list of message data type names prepended with the subject-ID;
    the subject-ID may be omitted if the data type defines a fixed one:

    \b
        [SUBJECT_ID:]TYPE_NAME[.MAJOR[.MINOR]]

    If multiple subjects are specified, a synchronous subscription will be used.
    It is useful for subscribing to a group of coupled subjects like lockstep sensor feeds,
    but it will not work for subjects that are temporally unrelated or published at different rates.

    Each object emitted into stdout is a key-value mapping where the number of elements equals the number
    of subjects the command is asked to subscribe to;
    the keys are subject-IDs and values are the received message objects.

    In data type names forward or backward slashes can be used instead of ".";
    version numbers can be also separated using underscores.
    This is done to allow the user to rely on filesystem autocompletion when typing the command.

    Examples:

    \b
        yakut sub 33:uavcan.si.unit.angle.Scalar --with-metadata
    """
    _logger.debug("subject=%r, with_metadata=%r, count=%r", subject, with_metadata, count)
    if not subject:
        _logger.info("Nothing to do because no subjects are specified")
        return
    if count is not None and count <= 0:
        _logger.info("Nothing to do because count=%s", count)
        return

    count = count if count is not None else sys.maxsize
    formatter = purser.make_formatter()

    transport = purser.get_transport()
    if transport.local_node_id is not None:
        _logger.info("It is recommended to use an anonymous transport with this command.")

    with contextlib.closing(Presentation(transport)) as presentation:
        subscriber = _make_subscriber(subject, presentation)
        try:
            await _run(subscriber, formatter, with_metadata=with_metadata, count=count)
        finally:
            if _logger.isEnabledFor(logging.INFO):
                _logger.info("%s", presentation.transport.sample_statistics())
                _logger.info("%s", subscriber.sample_statistics())


def _make_subscriber(subjects: Sequence[str], presentation: Presentation) -> Subscriber[Any]:
    group = [_construct_port_id_and_type(ds) for ds in subjects]
    assert len(group) > 0
    if len(group) == 1:
        ((subject_id, dtype),) = group
        return presentation.make_subscriber(dtype, subject_id)
    raise NotImplementedError(
        "Multi-subject subscription is not yet implemented. See https://github.com/OpenCyphal/pycyphal/issues/65"
    )


def _construct_port_id_and_type(raw_specifier: str) -> tuple[int, Any]:
    subject_spec_parts = raw_specifier.split(":")
    if len(subject_spec_parts) == 2:
        return int(subject_spec_parts[0]), dtype_loader.load_dtype(subject_spec_parts[1])
    if len(subject_spec_parts) == 1:
        dtype = dtype_loader.load_dtype(subject_spec_parts[0])
        fpid = pycyphal.dsdl.get_fixed_port_id(dtype)
        if fpid is None:
            raise click.ClickException(f"{subject_spec_parts[0]} has no fixed port-ID")
        return fpid, dtype
    raise click.BadParameter(f"Invalid subject specifier: {raw_specifier!r}")


async def _run(subscriber: Subscriber[Any], formatter: Formatter, with_metadata: bool, count: int) -> None:
    async for msg, transfer in subscriber:
        assert isinstance(transfer, pycyphal.transport.TransferFrom)
        outer: dict[int, dict[str, Any]] = {}

        bi: dict[str, Any] = {}  # We use updates to ensure proper dict ordering: metadata before data
        if with_metadata:
            bi.update(convert_transfer_metadata_to_builtin(transfer))
        bi.update(pycyphal.dsdl.to_builtin(msg))
        outer[subscriber.port_id] = bi

        print(formatter(outer))

        count -= 1
        if count <= 0:
            _logger.debug("Reached the specified message count, stopping")
            break
