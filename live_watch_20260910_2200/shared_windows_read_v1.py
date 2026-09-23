"""Bounded Windows reader permitting atomic replacement by the source writer."""
import ctypes
from ctypes import wintypes
import msvcrt
import os

def read_shared(path, limit):
    if type(limit) is not int or limit <= 0 or limit > 32*1024*1024:
        raise ValueError('read_limit')
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    create=kernel.CreateFileW
    create.argtypes=[wintypes.LPCWSTR,wintypes.DWORD,wintypes.DWORD,ctypes.c_void_p,wintypes.DWORD,wintypes.DWORD,wintypes.HANDLE]
    create.restype=wintypes.HANDLE
    close=kernel.CloseHandle;close.argtypes=[wintypes.HANDLE];close.restype=wintypes.BOOL
    handle=create(str(path),0x80000000,0x1|0x2|0x4,None,3,0x80,None)
    if handle in (None,wintypes.HANDLE(-1).value):raise OSError(ctypes.get_last_error(),'shared_read_open_failed')
    try:fd=msvcrt.open_osfhandle(handle,os.O_RDONLY|os.O_BINARY)
    except Exception:
        close(handle);raise
    with os.fdopen(fd,'rb') as stream:return stream.read(limit)
