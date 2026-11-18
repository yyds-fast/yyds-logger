import builtins
import os
import sys


def _enable_windows_vt(stream):
    try:
        import ctypes
        import msvcrt

        fileno = stream.fileno()
        handle = msvcrt.get_osfhandle(fileno)
        kernel32 = ctypes.windll.kernel32
        mode = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        enable_vt = 0x0004  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
        if mode.value & enable_vt:
            return True
        return bool(kernel32.SetConsoleMode(handle, mode.value | enable_vt))
    except Exception:
        return False


def _has_colorama():
    try:
        import colorama  # noqa: F401
    except ImportError:
        return False
    return True


def should_colorize(stream):
    if stream is None:
        return False

    if getattr(builtins, "__IPYTHON__", False) and (stream is sys.stdout or stream is sys.stderr):
        try:
            import ipykernel
            import IPython

            ipython = IPython.get_ipython()
            is_jupyter_stream = isinstance(stream, ipykernel.iostream.OutStream)
            is_jupyter_shell = isinstance(ipython, ipykernel.zmqshell.ZMQInteractiveShell)
        except Exception:
            pass
        else:
            if is_jupyter_stream and is_jupyter_shell:
                return True

    if stream is sys.__stdout__ or stream is sys.__stderr__:
        if "CI" in os.environ and any(
            ci in os.environ
            for ci in ["TRAVIS", "CIRCLECI", "APPVEYOR", "GITLAB_CI", "GITHUB_ACTIONS"]
        ):
            return True
        if "PYCHARM_HOSTED" in os.environ:
            return True
        if os.name == "nt" and "TERM" in os.environ:
            return True

    try:
        is_tty = stream.isatty()
    except Exception:
        return False

    if not is_tty:
        return False

    if os.name == "nt" and (stream is sys.__stdout__ or stream is sys.__stderr__):
        return _enable_windows_vt(stream) or _has_colorama()

    return True


def should_wrap(stream):
    if os.name != "nt":
        return False

    if stream is not sys.__stdout__ and stream is not sys.__stderr__:
        return False

    if _enable_windows_vt(stream):
        return False

    try:
        from colorama.win32 import winapi_test
    except ImportError:
        return False

    if not winapi_test():
        return False

    try:
        from colorama.winterm import enable_vt_processing
    except ImportError:
        return True

    try:
        return not enable_vt_processing(stream.fileno())
    except Exception:
        return True


def wrap(stream):
    try:
        from colorama import AnsiToWin32
    except ImportError:
        return stream

    return AnsiToWin32(stream, convert=True, strip=True, autoreset=False).stream
