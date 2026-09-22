#!/usr/bin/env python3
"""Exercise the compiled collector, real UDP input, HTTP isolation and profiles."""
import pathlib
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


def port(kind=socket.SOCK_STREAM):
    with socket.socket(socket.AF_INET, kind) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read()


def run(binary, enabled):
    http, udp, profile = port(), port(socket.SOCK_DGRAM), port()
    public = f'http://127.0.0.1:{http}'
    local = f'http://127.0.0.1:{profile}'
    args = [binary, '-listen', f'sflow://127.0.0.1:{udp}?count=1&workers=2&queue_size=32',
            '-addr', f'127.0.0.1:{http}', '-format', 'bin']
    if enabled:
        args += ['-diagnostics', '-diagnostics.sample-every=1',
                 '-diagnostics.pprof.addr', f'127.0.0.1:{profile}']
    with tempfile.TemporaryFile() as errors:
        process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=errors)
        try:
            for _ in range(100):
                if process.poll() is not None:
                    errors.seek(0)
                    raise RuntimeError(errors.read().decode())
                try:
                    if get(public + '/__health') == b'OK\n':
                        break
                except (OSError, urllib.error.HTTPError):
                    pass
                time.sleep(.05)
            else:
                raise RuntimeError('collector did not become healthy')
            for endpoint in ['/debug/pprof/', '/debug/pprof/goroutine', '/debug/pprof/profile']:
                try:
                    get(public + endpoint)
                    raise AssertionError('profiling exposed on public listener')
                except urllib.error.HTTPError as error:
                    assert error.code == 404, error
            # Valid sFlow v5 datagram with one empty flow sample: still produces
            # one protobuf flow message and exercises every stage including Send.
            payload = struct.pack('!17I', 5, 1, 0x7f000001, 0, 1, 1000, 1,
                                  1, 32, 1, 0, 1, 1, 0, 0, 0, 0)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
                for _ in range(10):
                    sender.sendto(payload, ('127.0.0.1', udp))
            for _ in range(100):
                text = get(public + '/metrics').decode()
                if ('stage="kafka_enqueue"' in text if enabled else 'goflow2_flow_process_sf_total{' in text):
                    break
                time.sleep(.02)
            assert 'goflow2_flow_process_sf_total' in text
            if enabled:
                for stage in ['queue_wait', 'handling', 'decode', 'produce', 'producer_metrics',
                              'format', 'kafka_enqueue', 'wrapper_metrics']:
                    assert f'stage="{stage}"' in text, stage
                for name in ['goflow_diagnostics_socket_datagrams_total',
                             'goflow_diagnostics_build_info', 'go_sched_latencies_seconds_bucket',
                             'go_sync_mutex_wait_total_seconds_total']:
                    assert name in text, name
                assert b'goroutine profile' in get(local + '/debug/pprof/goroutine?debug=1')
                for endpoint in ['heap', 'mutex?seconds=1', 'block?seconds=1', 'profile?seconds=1']:
                    assert get(local + '/debug/pprof/' + endpoint).startswith(b'\x1f\x8b'), endpoint
            else:
                assert 'goflow_diagnostics_' not in text
            print('diagnostics enabled=' + str(enabled) + ': UDP, metrics and HTTP isolation OK')
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError('collector shutdown stalled')


if __name__ == '__main__':
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    run(binary, False)
    run(binary, True)
