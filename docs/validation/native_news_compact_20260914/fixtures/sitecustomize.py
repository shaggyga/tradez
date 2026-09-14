"""Deny external network and new child processes inside this synthetic test kit."""
import os
import socket
import subprocess
import sys


def denied(*args, **kwargs):
    raise RuntimeError('portable_native_news_reproduction_external_action_forbidden')


for name in ('connect', 'connect_ex', 'sendto', 'sendmsg'):
    if hasattr(socket.socket, name):
        setattr(socket.socket, name, denied)
socket.create_connection = denied
socket.getaddrinfo = denied
subprocess.Popen = denied
os.system = denied
if hasattr(os, 'startfile'):
    os.startfile = denied
sys._native_news_reproduction_external_actions_blocked = True
