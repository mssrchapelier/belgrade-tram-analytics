import asyncio
import logging
from asyncio import AbstractEventLoop
from logging import Logger
from typing import Dict, Self

from aiohttp import ClientSession

from common.utils.logging_utils.logging_utils import get_logger_name_for_object


class AsyncSessionProvider:

    """
    Creates and stores an `aiohttp` async client session PER EVENT LOOP.

    RATIONALE: Encountered issues when trying to use the session
    injected into `v1.src.dashboard.dashboard.AsyncLiveUpdateGetter`
    in a method called from outside of the event loop
    in which the session was created.
    To overcome this, creating a session instance per loop
    and injecting this provider instead.
    """

    def __init__(self):
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        self._loop_id_to_session: Dict[int, ClientSession] = dict()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Closes and deregisters all open sessions.
        """
        # close all open sessions
        for session in self._loop_id_to_session.values(): # type: ClientSession
            if not session.closed:
                await session.close()
        # clear the state
        self._loop_id_to_session.clear()

    async def get_session(self) -> ClientSession:
        """
        Get the session for the event loop from which this coroutine is called.
        If a session does not exist for this loop, creates one first.
        """
        loop: AbstractEventLoop = asyncio.get_running_loop()
        loop_id: int = id(loop)

        # try getting the session for this loop
        session: ClientSession | None = self._loop_id_to_session.get(loop_id)
        if session is None or session.closed:
            # create a new one if one doesn't exist for this loop
            session = ClientSession()
            self._loop_id_to_session[loop_id] = session
            self._logger.debug(f"Created a new session for loop ID: {loop_id}")
        return session
