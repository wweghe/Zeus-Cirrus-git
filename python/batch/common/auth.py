from typing import Callable, Any


class AuthTokenRefeshDecorator:

    def __init__(self, 
                 func : Callable, 
                 token_refresh_func : Callable
                ) -> None:
        self._func = func
        self._token_refresh_func = token_refresh_func


    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._token_refresh_func()
        return self._func(*args, **kwargs)
    