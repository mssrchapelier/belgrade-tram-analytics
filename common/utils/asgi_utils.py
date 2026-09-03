from typing import Any, AsyncGenerator, Callable
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager

from starlette.types import Lifespan
from starlette.applications import Starlette
from starlette.routing import BaseRoute, Mount, Router
from starlette.types import ASGIApp


@asynccontextmanager
async def combined_lifespan[MainAppOwnStateType: Mapping[str, Any] | None](
        app: Starlette,
        own_lifespan: Callable[[Starlette], AbstractAsyncContextManager[MainAppOwnStateType]]
) -> AsyncGenerator[MainAppOwnStateType]:
    """
    Combine lifespans of all mounted sub-applications and an additional provided lifespan (`own_lifespan`).
    First the lifespans of the sub-applications will be entered, then `own_lifespan`.
    On shutdown, the order is reversed.

    See `make_partial_lifespan()` for an example of use.

    Notes:
        Adapted from a solution by GitHub user @unights.
        See: https://github.com/fastapi/fastapi/issues/811#issuecomment-1870030103
    """
    async with AsyncExitStack() as stack: # type: AsyncExitStack
        for route in app.routes: # type: BaseRoute
            if isinstance(route, Mount):
                mounted_app: ASGIApp | Router | None = route.app
                if isinstance(mounted_app, Starlette):
                    mounted_lifespan: Lifespan[Any] = mounted_app.router.lifespan_context
                    await stack.enter_async_context(mounted_lifespan(mounted_app))
        yielded: MainAppOwnStateType = await stack.enter_async_context(own_lifespan(app))
        yield yielded


def make_partial_combined_lifespan[MainAppOwnStateType: Mapping[str, Any] | None](
        own_lifespan: Callable[[Starlette], AbstractAsyncContextManager[MainAppOwnStateType]]
) -> Callable[[Starlette], AbstractAsyncContextManager[MainAppOwnStateType]]:
    """
    Construct a partial lifespan callable for the main application given `own_lifespan` 
    (the lifespan events meant to be tied to the main application itself, not to the sub-applications).
    Invoke the partial with `own_lifespan` to get the combined lifespan callable
    to be passed to the constructor of the main application.

    Example:
    ```
    @asynccontextmanager
    async def main_app_own_lifespan(app: Starlette) -> AsyncGenerator[MainStateType]:
        s: MainStateType = MainStateType()
        yield s

    @asynccontextmanager
    async def subapp_lifespan(app: Starlette) -> AsyncGenerator[None]:
        yield None

    sub_app: Starlette = Starlette(lifespan=subapp_lifespan)

    # ... define multiple sub-applications as needed ...

    main_routes: List[BaseRoute] = [
        ...
        Mount("/sub", app=sub_app),
        # ... mount other sub-applications ...
    ]
    main_app_full_lifespan: Callable[[Starlette], AbstractAsyncContextManager[MainStateType]] = make_partial_lifespan(main_app_own_lifespan)
    main_app: Starlette = Starlette(lifespan=main_app_full_lifespan)

    # On the startup of main_app, first subapp_lifespan will be entered, then main_app_own_lifespan.
    # On the shutdown of main_app, first main_app_own_lifespan will be exited, then subapp_lifespan.
    ```
    """

    @asynccontextmanager
    async def wrapped(app: Starlette) -> AsyncGenerator[MainAppOwnStateType]:
        async with combined_lifespan(app, own_lifespan) as state: # type: MainAppOwnStateType
            yield state

    return wrapped
