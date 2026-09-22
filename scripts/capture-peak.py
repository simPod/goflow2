#!/usr/bin/env python3
"""Bounded, local GoFlow2 peak captures; Python 3 standard library only."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request

MIB = 1024 * 1024
SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z_0-9:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+\S+)?\s*$')
LABEL = re.compile(r'\s*([a-zA-Z_][a-zA-Z_0-9]*)\s*=\s*"((?:[^"\\]|\\[\\"n])*)"\s*(?:,|$)')
QUEUE = 'goflow_diagnostics_queue_'
DROP = 'goflow2_flow_dropped_packets_total'


def utc():
    return datetime.now(timezone.utc).isoformat()


def loopback_url(value):
    try:
        url = urllib.parse.urlsplit(value)
        if (url.scheme != 'http' or not ipaddress.ip_address(url.hostname).is_loopback
                or url.username is not None or url.password is not None
                or url.query or url.fragment or not url.port):
            raise ValueError()
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError('use HTTP with a literal loopback IP and port')
    return value.rstrip('/')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError('HTTP redirects are forbidden')


def fetch(url, limit, destination=None, stop=None):
    """Stream to a file, or return bounded bytes. Never use proxies or redirects."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    data = bytearray()
    deadline = time.monotonic() + 65
    try:
        with opener.open(url, timeout=40) as response:
            expected = int(response.headers.get('Content-Length', '-1'))
            if expected > limit:
                raise ValueError('response exceeds byte limit')
            output = destination.open('xb') if destination else None
            try:
                size = 0
                while True:
                    if (stop and stop.is_set()) or time.monotonic() > deadline:
                        raise TimeoutError('HTTP capture stopped or exceeded deadline')
                    chunk = response.read1(min(65536, limit - size + 1))
                    if not chunk:
                        if expected >= 0 and size != expected:
                            raise ValueError('incomplete HTTP response')
                        break
                    size += len(chunk)
                    if size > limit:
                        raise ValueError('response exceeds byte limit')
                    if output:
                        output.write(chunk)
                    else:
                        data.extend(chunk)
            finally:
                if output:
                    output.close()
    except Exception:
        if destination:
            destination.unlink(missing_ok=True)
        raise
    return bytes(data)


def metrics(raw, listener):
    queues, drops, start = {}, {}, None
    for line in raw.decode('utf-8').splitlines():
        if not line or line.startswith('#'):
            continue
        match = SAMPLE.fullmatch(line)
        if not match:
            continue
        name, text, number = match.groups()
        if name not in (QUEUE + 'length', QUEUE + 'capacity', DROP, 'process_start_time_seconds'):
            continue
        labels, pos = {}, 0
        text = text or ''
        while pos < len(text):
            label = LABEL.match(text, pos)
            if not label or label[1] in labels:
                raise ValueError('invalid metric labels')
            labels[label[1]] = re.sub(r'\\([\\"n])', lambda m: '\n' if m[1] == 'n' else m[1], label[2])
            pos = label.end()
        value = float(number)
        if not math.isfinite(value) or value < 0:
            raise ValueError('invalid diagnostic metric value')
        if name.startswith(QUEUE) and labels.get('listener') == listener:
            queues[name] = value
        elif name == DROP:
            drops[tuple(sorted(labels.items()))] = value
        elif name == 'process_start_time_seconds':
            start = value
    if QUEUE + 'length' not in queues or queues.get(QUEUE + 'capacity', 0) <= 0:
        raise ValueError('diagnostics queue length/capacity absent or capacity zero for ' + listener)
    return {'queue_ratio': queues[QUEUE + 'length'] / queues[QUEUE + 'capacity'],
            'drops': drops, 'process_start_time': start}


class Trigger:
    def __init__(self, args):
        self.args, self.previous = args, None
        self.last, self.last_baseline, self.count = None, None, 0

    def observe(self, sample, now):
        previous, self.previous = self.previous, sample
        increase = 0
        if previous and previous['process_start_time'] == sample['process_start_time']:
            increase = sum(max(0, value - previous['drops'].get(key, 0))
                           for key, value in sample['drops'].items()
                           if key in previous['drops'] or sample['process_start_time'] is not None)
        if self.count >= self.args.max_bundles:
            return None
        if self.last is None:
            return 'baseline'
        if now - self.last < self.args.cooldown:
            return None
        if now - self.last_baseline >= self.args.baseline_interval:
            return 'periodic-baseline'
        if sample['queue_ratio'] >= .75:
            return 'queue'
        return 'drops' if increase > 0 else None

    def captured(self, reason, now):
        self.count += 1
        self.last = now
        if 'baseline' in reason:
            self.last_baseline = now


def command(argv, destination, stop, limit=16 * MIB):
    """Bound subprocess time and output; no shell, environment dump, or cmdline."""
    if not shutil.which(argv[0]):
        raise FileNotFoundError(argv[0] + ' unavailable; capture skipped')
    with destination.open('xb') as output, subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
        try:
            deadline, size = time.monotonic() + 40, 0
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while True:
                    if stop.is_set() or time.monotonic() > deadline:
                        raise TimeoutError('command stopped or exceeded deadline')
                    if not selector.select(.2):
                        continue
                    chunk = os.read(process.stdout.fileno(), min(65536, limit - size + 1))
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > limit:
                        raise ValueError('command exceeds byte limit')
                    output.write(chunk)
            if process.wait(timeout=1):
                raise RuntimeError('command returned nonzero status')
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()


class Capture:
    def __init__(self, args):
        self.args, self.stop = args, threading.Event()
        self.root = Path(args.output)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.lock, self.errors = threading.Lock(), 0
        self.captures, self.metric_record = {}, None

    def error(self, message):
        with self.lock:
            self.errors += 1
            line = utc() + ' ERROR: ' + message
            print(line, file=sys.stderr, flush=True)
            with (self.root / 'errors.log').open('a') as output:
                output.write(line + '\n')

    def attempt(self, label, function, *args):
        record = {'started_utc': utc(), 'status': 'stopped'}
        try:
            if not self.stop.is_set():
                result = function(*args)
                record['status'] = 'ok'
                return result
        except Exception as exc:
            record.update(status='stopped' if self.stop.is_set() else 'error', error=str(exc))
            self.error(label + ': ' + str(exc))
        finally:
            record['finished_utc'] = utc()
            with self.lock:
                self.captures[label] = record

    def scrape(self):
        started = utc()
        raw = fetch(self.args.metrics_url, self.args.metric_bytes, stop=self.stop)
        self.metric_record = {'started_utc': started, 'finished_utc': utc(), 'status': 'ok'}
        return raw, metrics(raw, self.args.listener)

    def proc(self, folder):
        names = ['/proc/net/snmp', '/proc/net/netstat', '/proc/softirqs']
        if self.args.pid:
            names += [f'/proc/{self.args.pid}/{name}' for name in
                      ('status', 'stat', 'sched', 'schedstat', 'limits', 'cgroup')]
        for name in names:
            def copy(name=name):
                with open(name, 'rb') as source:
                    data = source.read(16 * MIB + 1)
                if len(data) > 16 * MIB:
                    raise ValueError('proc file exceeds byte limit')
                (folder / name.lstrip('/').replace('/', '-')).write_bytes(data)
            self.attempt(folder.name + ':' + name, copy)
        if self.args.thread_schedstats and self.args.pid:
            def threads():
                with (folder / 'thread-schedstats.txt').open('x') as output:
                    with os.scandir(f'/proc/{self.args.pid}/task') as entries:
                        for index, entry in enumerate(entries):
                            if index >= 256:
                                raise ValueError('thread schedstats capped at 256 threads')
                            def copy_thread():
                                with open(entry.path + '/schedstat') as source:
                                    output.write(entry.name + ' ' + source.read(4096) + '\n')
                            self.attempt(folder.name + ':thread ' + entry.name, copy_thread)
            self.attempt(folder.name + ':thread schedstats', threads)

    def snapshot(self, folder, phase):
        target = folder / phase
        target.mkdir(mode=0o700)
        self.proc(target)
        self.attempt('goroutine ' + phase, fetch,
                     self.args.pprof_url + '/goroutine?debug=2', self.args.profile_bytes,
                     target / 'goroutine-debug2.txt', self.stop)

    def bundle(self, number, reason, raw, sample):
        folder = self.root / f'{number:02d}-{reason}'
        folder.mkdir(mode=0o700)
        started, errors = time.time(), self.errors
        self.captures = {'metrics before': self.metric_record} if self.metric_record else {}
        meta = {'started_utc': utc(), 'reason': reason, 'pid': self.args.pid,
                'pid_policy': 'fixed; restart capture with the new --pid after a process restart',
                'queue_ratio': sample['queue_ratio'], 'process_start_time': sample['process_start_time'],
                'process_uptime_seconds': started - sample['process_start_time']
                if sample['process_start_time'] is not None else None}
        (folder / 'metrics-before.txt').write_bytes(raw)
        self.snapshot(folder, 'before')
        profiles = [('cpu', 'profile?seconds=30'), ('mutex', 'mutex?seconds=30'),
                    ('block', 'block?seconds=30'), ('allocs', 'allocs?seconds=30')]
        immediate = [('heap', 'heap'), ('goroutine', 'goroutine')]
        if self.args.trace_seconds:
            immediate.insert(0, ('trace', f'trace?seconds={self.args.trace_seconds}'))
        # Six auxiliary slots: trace, heap, goroutine, two ss calls, and pidstat.
        with ThreadPoolExecutor(max_workers=self.args.workers) as pool, ThreadPoolExecutor(max_workers=6) as aux:
            futures = [aux.submit(self.attempt, name, fetch, self.args.pprof_url + '/' + endpoint,
                                   self.args.profile_bytes, folder / (name + ('.out' if name == 'trace' else '.pb')), self.stop)
                       for name, endpoint in immediate]
            for protocol in ('u', 't'):
                futures.append(aux.submit(self.attempt, 'ss ' + protocol, command,
                                          ['ss', '-' + protocol, '-a', '-n', '-m', '-p'],
                                          folder / ('ss-' + protocol + '.txt'), self.stop))
            if self.args.pid:
                futures.append(aux.submit(self.attempt, 'pidstat', command,
                                          ['pidstat', '-u', '-w', '-t', '-p', str(self.args.pid), '1', '30'],
                                          folder / 'pidstat.txt', self.stop))
            futures += [pool.submit(self.attempt, name, fetch, self.args.pprof_url + '/' + endpoint,
                                    self.args.profile_bytes, folder / (name + '.pb'), self.stop)
                        for name, endpoint in profiles]
            for future in futures:
                future.result()
        self.snapshot(folder, 'after')
        after_raw = self.attempt('metrics after', fetch, self.args.metrics_url,
                                 self.args.metric_bytes, None, self.stop)
        after = None
        if after_raw is not None:
            (folder / 'metrics-after.txt').write_bytes(after_raw)
            after = self.attempt('parse metrics after', metrics, after_raw, self.args.listener)
        if after:
            meta['process_start_time_after'] = after['process_start_time']
        meta.update(finished_utc=utc(), elapsed_seconds=time.time() - started,
                    errors=self.errors - errors, interrupted=self.stop.is_set(), captures=self.captures)
        (folder / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
        return after if after else sample

    def run(self):
        deadline = time.monotonic() + self.args.duration
        trigger = Trigger(self.args)
        try:
            raw, sample = self.scrape()
            fetch(self.args.pprof_url + '/', self.args.metric_bytes, stop=self.stop)
        except Exception as exc:
            self.error('STARTUP FAILED: ' + str(exc))
            return 1
        failures = 0
        while not self.stop.is_set() and time.monotonic() < deadline:
            reason = trigger.observe(sample, time.monotonic())
            if reason:
                trigger.captured(reason, time.monotonic())
                print(f'{utc()} bundle {trigger.count}/{self.args.max_bundles}: {reason}', flush=True)
                trigger.previous = self.bundle(trigger.count, reason, raw, sample)
                if trigger.count >= self.args.max_bundles:
                    print('Bundle limit reached; capture finished.', flush=True)
                    break
            while not self.stop.wait(min(self.args.poll_interval, max(0, deadline - time.monotonic()))):
                if time.monotonic() >= deadline:
                    break
                try:
                    raw, sample = self.scrape()
                    failures = 0
                    break
                except Exception as exc:
                    failures += 1
                    self.error(f'METRICS FAILURE #{failures}; capture blind: {exc}')
        print(f'{utc()} finished: {trigger.count} bundles, {self.errors} errors; {self.root}', flush=True)
        return 1 if self.errors else 0


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='peak-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    parser.add_argument('--metrics-url', type=loopback_url, default='http://127.0.0.1:8081/metrics')
    parser.add_argument('--pprof-url', type=loopback_url, default='http://127.0.0.1:6060/debug/pprof')
    parser.add_argument('--listener', default='sflow://:9801')
    parser.add_argument('--pid', type=int, help='fixed GoFlow2 PID for /proc and pidstat; restart capture with new PID after restart')
    parser.add_argument('--thread-schedstats', action='store_true', help='read at most 256 thread schedstats; requires --pid')
    for name, default in [('duration', 86400), ('poll-interval', 10), ('cooldown', 900),
                          ('baseline-interval', 3600), ('max-bundles', 24), ('workers', 4),
                          ('profile-bytes', 128 * MIB), ('metric-bytes', 16 * MIB)]:
        parser.add_argument('--' + name, type=int, default=default,
                            help=f'default {default}; time values are seconds')
    parser.add_argument('--trace-seconds', type=int, default=0, help='expensive: 0 disables, or 1–5 seconds')
    args = parser.parse_args(argv)
    if any(getattr(args, key) <= 0 for key in ('duration', 'poll_interval', 'cooldown', 'baseline_interval',
                                             'max_bundles', 'workers', 'profile_bytes', 'metric_bytes')):
        parser.error('limits and intervals must be positive')
    if (args.max_bundles > 24 or args.workers > 8 or args.profile_bytes > 128 * MIB
            or args.metric_bytes > 16 * MIB or not 0 <= args.trace_seconds <= 5):
        parser.error('maximums: 24 bundles, 8 workers, 128MiB/profile, 16MiB/metrics, 5s trace')
    if (args.pid is not None and args.pid <= 0) or (args.thread_schedstats and not args.pid):
        parser.error('use a positive --pid; --thread-schedstats requires --pid')
    return args


def main():
    os.umask(0o077)
    args = arguments()
    capture = None
    try:
        capture = Capture(args)
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: capture.stop.set())
        return capture.run()
    except Exception as exc:
        if capture:
            try:
                capture.error('FATAL: ' + str(exc))
                return 1
            except OSError:
                pass
        print(f'{utc()} FATAL: {exc}', file=sys.stderr, flush=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
