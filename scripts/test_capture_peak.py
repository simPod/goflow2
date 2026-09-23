import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace


spec = importlib.util.spec_from_file_location('capture_peak', Path(__file__).with_name('capture-peak.py'))
peak = importlib.util.module_from_spec(spec)
spec.loader.exec_module(peak)


def exposition(length=1, drops=10, start=100):
    return (f'# complete raw metrics retained\n'
            f'goflow_diagnostics_queue_length{{listener="sflow://:9801"}} {length}\n'
            'goflow_diagnostics_queue_capacity{listener="sflow://:9801"} 100\n'
            f'goflow2_flow_dropped_packets_total{{remote_ip="192.0.2.1",type="sflow"}} {drops}\n'
            f'process_start_time_seconds {start}\n'
            'unrelated_metric 123\n').encode()


class MockServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 16

    def __init__(self):
        super().__init__(('127.0.0.1', 0), Handler)
        self.responses = {}
        self.requests = []
        self.active = self.maximum = 0
        self.lock = threading.Lock()
        self.on_request = lambda path: None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        with self.server.lock:
            self.server.requests.append(self.path)
            self.server.active += 1
            self.server.maximum = max(self.server.maximum, self.server.active)
        try:
            self.server.on_request(self.path)
            time.sleep(.01)
            default = exposition() if self.path == '/metrics' else b'profile bytes'
            status, body, headers = self.server.responses.get(self.path, (200, default, {}))
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with self.server.lock:
                self.server.active -= 1


class CaptureTest(unittest.TestCase):
    def setUp(self):
        usage = mock.patch.object(peak.shutil, 'disk_usage', return_value=SimpleNamespace(free=100 * 1024 ** 3))
        self.disk_usage = usage.start()
        self.addCleanup(usage.stop)
        self.umask = os.umask(0o077)
        self.tmp = tempfile.TemporaryDirectory()
        self.server = MockServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'
        self.args = peak.arguments(['--output', self.tmp.name + '/capture',
                                    '--metrics-url', self.url + '/metrics',
                                    '--pprof-url', self.url + '/debug/pprof', '--max-bundles', '1'])
        self.quiet = io.StringIO()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.tmp.cleanup()
        os.umask(self.umask)

    def sample(self, **kwargs):
        self.server.responses['/metrics'] = (200, exposition(**kwargs), {})
        return peak.metrics(peak.fetch(self.url + '/metrics', 4096), self.args.listener)

    def test_queue_and_drop_triggers_from_http(self):
        self.args.max_bundles = 24
        trigger = peak.Trigger(self.args)
        self.assertEqual(trigger.observe(self.sample(), 0), 'baseline')
        trigger.captured('baseline', 0)
        self.assertIsNone(trigger.observe(self.sample(length=75), 899))
        self.assertEqual(trigger.observe(self.sample(length=75), 900), 'queue')
        trigger.captured('queue', 900)
        self.assertEqual(trigger.observe(self.sample(drops=11), 1800), 'drops')
        trigger.captured('drops', 1800)
        self.assertIsNone(trigger.observe(self.sample(drops=11), 2700))
        self.assertEqual(trigger.observe(self.sample(drops=11), 3600), 'periodic-baseline')

    def test_resets_and_exporter_churn(self):
        self.args.max_bundles = 24
        trigger = peak.Trigger(self.args)
        trigger.observe(self.sample(drops=100), 0)
        trigger.captured('baseline', 0)
        self.assertIsNone(trigger.observe(self.sample(drops=1), 900))
        self.assertIsNone(trigger.observe(self.sample(drops=1000, start=200), 910))
        self.assertEqual(trigger.observe(self.sample(drops=1001, start=200), 920), 'drops')
        sample = self.sample(drops=1001, start=200)
        old_key = next(iter(sample['drops']))
        new_key = (('remote_ip', '192.0.2.2'),)
        sample['drops'] = {new_key: 999999}
        self.assertEqual(trigger.observe(sample, 930), 'drops')
        # A large disappearing series must not hide an increase in a surviving series.
        sample = dict(sample, drops={old_key: 500000, new_key: 999999})
        self.assertEqual(trigger.observe(sample, 940), 'drops')
        self.assertEqual(trigger.observe(dict(sample, drops={new_key: 1000000}), 950), 'drops')

    def test_first_lazy_drop_series_requires_known_unchanged_process(self):
        self.args.max_bundles = 24
        for previous_start, current_start, expected in ((100, 100, 'drops'), (100, 200, None),
                                                       (None, None, None), (None, 100, None)):
            with self.subTest(previous_start=previous_start, current_start=current_start):
                trigger = peak.Trigger(self.args)
                raw = b'\n'.join(line for line in exposition().splitlines()
                                 if not line.startswith(peak.DROP.encode()))
                self.server.responses['/metrics'] = (200, raw, {})
                before = peak.metrics(peak.fetch(self.url + '/metrics', 4096), self.args.listener)
                before['process_start_time'] = previous_start
                trigger.observe(before, 0)
                trigger.captured('baseline', 0)
                after = self.sample(drops=1)
                after['process_start_time'] = current_start
                self.assertEqual(trigger.observe(after, 900), expected)
                self.assertIsNone(trigger.observe(after, 910))

    def test_parser_labels_floats_and_listener_selection(self):
        raw = (b'goflow_diagnostics_queue_length{listener="other"} 99\n'
               b'goflow_diagnostics_queue_capacity{listener="other"} 100\n'
               b'goflow_diagnostics_queue_length{listener="sflow://:9801"} 7.5e1 123\n'
               b'goflow_diagnostics_queue_capacity{listener="sflow://:9801"} 1e2\n'
               b'goflow2_flow_dropped_packets_total{note="a,}b\\\"c\\\\d\\ne", remote_ip="::1"} 2.0\n')
        sample = peak.metrics(raw, self.args.listener)
        self.assertEqual(sample['queue_ratio'], .75)
        key = next(iter(sample['drops']))
        self.assertEqual(dict(key)['note'], 'a,}b"c\\d\ne')
        with self.assertRaises(ValueError):
            peak.metrics(raw, 'missing')
        with self.assertRaises(ValueError):
            peak.metrics(exposition().replace(b'} 1\n', b'} NaN\n'), self.args.listener)

    def test_baseline_never_exceeds_cap(self):
        trigger = peak.Trigger(self.args)
        self.assertEqual(trigger.observe(self.sample(), 0), 'baseline')
        trigger.captured('baseline', 0)
        for now in (900, 3600, 86400):
            self.assertIsNone(trigger.observe(self.sample(length=100, drops=now), now))

    def run_capture(self):
        capture = peak.Capture(self.args)
        with mock.patch.object(capture, 'proc'), mock.patch.object(peak.shutil, 'which', return_value='/tool'), \
                mock.patch.object(peak, 'command'), redirect_stdout(self.quiet), redirect_stderr(self.quiet):
            result = capture.run()
        return capture, result

    def test_full_bundle_parameters_raw_metrics_permissions_and_cap(self):
        capture, result = self.run_capture()
        self.assertEqual(result, 0)
        folder = capture.root / '01-baseline'
        self.assertEqual((folder / 'metrics-before.txt').read_bytes(), exposition())
        self.assertEqual((folder / 'metrics-after.txt').read_bytes(), exposition())
        expected = ['profile?seconds=30', 'mutex?seconds=30', 'block?seconds=30',
                    'allocs?seconds=30', 'heap', 'goroutine']
        for endpoint in expected:
            self.assertEqual(self.server.requests.count('/debug/pprof/' + endpoint), 1)
        self.assertEqual(self.server.requests.count('/debug/pprof/goroutine?debug=2'), 2)
        self.assertFalse(any('gc=' in path or '/trace' in path for path in self.server.requests))
        self.assertLessEqual(self.server.maximum, self.args.workers + 2)
        self.assertGreater(self.server.maximum, 1)
        self.assertEqual(len(list(capture.root.glob('*-baseline'))), 1)
        metadata = json.loads((folder / 'metadata.json').read_text())
        self.assertGreater(metadata['process_uptime_seconds'], 0)
        self.assertEqual(metadata['process_start_time_after'], 100)
        self.assertEqual(metadata['errors'], 0)
        captures = metadata['captures']
        for name in ('cpu', 'mutex', 'block', 'allocs', 'heap', 'goroutine', 'ss u', 'ss t',
                     'goroutine before', 'goroutine after', 'metrics before', 'metrics after'):
            self.assertEqual(captures[name]['status'], 'ok')
            start = peak.datetime.fromisoformat(captures[name]['started_utc'])
            end = peak.datetime.fromisoformat(captures[name]['finished_utc'])
            self.assertEqual(start.utcoffset().total_seconds(), 0)
            self.assertGreaterEqual(end, start)
        for path in [capture.root, *capture.root.rglob('*')]:
            self.assertEqual(path.stat().st_mode & 0o777, 0o700 if path.is_dir() else 0o600)
        self.assertEqual(capture.storage.total, sum(p.stat().st_size for p in capture.root.rglob('*') if p.is_file()))
        self.assertIn(folder, capture.storage.completed)

    def test_explicit_trace(self):
        self.args.trace_seconds = 3
        _, result = self.run_capture()
        self.assertEqual(result, 0, self.quiet.getvalue())
        self.assertEqual(self.server.requests.count('/debug/pprof/trace?seconds=3'), 1)

    def test_auxiliary_captures_overlap_all_four_long_profiles(self):
        self.args.pid, self.args.trace_seconds = 12345, 1
        endpoints = {'profile?seconds=30': 'cpu', 'mutex?seconds=30': 'mutex',
                     'block?seconds=30': 'block', 'allocs?seconds=30': 'allocs',
                     'trace?seconds=1': 'trace', 'heap': 'heap', 'goroutine': 'goroutine'}
        expected = set(endpoints.values()) | {'ss u', 'ss t', 'pidstat'}
        entered, timed_out = set(), []
        lock, release = threading.Lock(), threading.Event()

        def enter(name):
            with lock:
                entered.add(name)
                if entered == expected:
                    release.set()
            # No operation may finish until all ten captures start. A shared
            # four-worker pool fails this barrier instead of hiding the delay.
            if not release.wait(5):
                with lock:
                    timed_out.append(name)

        def request(path):
            name = endpoints.get(path.removeprefix('/debug/pprof/'))
            if name:
                enter(name)

        def run_command(argv, destination, stop, limit, storage):
            if argv[0] == 'pidstat':
                self.assertEqual(argv, ['pidstat', '-u', '-w', '-t', '-p', '12345', '1', '30'])
                enter('pidstat')
            else:
                self.assertEqual(argv[2:], ['-a', '-n', '-m', '-p'])
                enter('ss ' + argv[1][1:])

        self.server.on_request = request
        capture = peak.Capture(self.args)
        with mock.patch.object(capture, 'proc'), mock.patch.object(peak, 'command', side_effect=run_command), \
                redirect_stdout(self.quiet), redirect_stderr(self.quiet):
            self.assertEqual(capture.run(), 0)
        self.assertEqual(entered, expected)
        self.assertEqual(timed_out, [])
        self.assertLessEqual(self.server.maximum, self.args.workers + 3)
        manifest = json.loads((capture.root / '01-baseline/metadata.json').read_text())['captures']
        self.assertLessEqual(max(manifest[name]['started_utc'] for name in expected),
                             min(manifest[name]['finished_utc'] for name in expected))
        self.assertTrue(all(manifest[name]['status'] == 'ok' for name in expected))

    def test_stopped_capture_has_manifest_entry(self):
        capture = peak.Capture(self.args)
        capture.stop.set()
        operation = mock.Mock()
        capture.attempt('cpu', operation)
        operation.assert_not_called()
        self.assertEqual(capture.captures['cpu']['status'], 'stopped')
        self.assertLessEqual(capture.captures['cpu']['started_utc'], capture.captures['cpu']['finished_utc'])

    def test_startup_missing_diagnostics_fails_before_profiles(self):
        self.server.responses['/metrics'] = (200, b'unrelated 1\n', {})
        capture, result = self.run_capture()
        self.assertEqual(result, 1)
        self.assertEqual(self.server.requests, ['/metrics'])
        self.assertIn('STARTUP FAILED', (capture.root / 'errors.log').read_text())

    def test_startup_pprof_unavailable(self):
        self.server.responses['/debug/pprof/'] = (404, b'absent', {})
        capture, result = self.run_capture()
        self.assertEqual(result, 1)
        self.assertFalse(list(capture.root.glob('*-baseline')))

    def test_profile_failure_recorded_and_other_profiles_complete(self):
        self.server.responses['/debug/pprof/mutex?seconds=30'] = (500, b'failure', {})
        capture, result = self.run_capture()
        self.assertEqual(result, 1)
        self.assertIn('mutex', (capture.root / 'errors.log').read_text())
        self.assertTrue((capture.root / '01-baseline/cpu.pb').exists())
        self.assertFalse((capture.root / '01-baseline/mutex.pb').exists())
        metadata = json.loads((capture.root / '01-baseline/metadata.json').read_text())
        self.assertEqual(metadata['errors'], 1)
        self.assertEqual(metadata['captures']['mutex']['status'], 'error')
        self.assertIn('500', metadata['captures']['mutex']['error'])

    def test_http_bounds_with_and_without_content_length(self):
        for headers in ({}, {'Content-Length': '1000'}):
            self.server.responses['/large'] = (200, b'x' * 1000, headers)
            destination = Path(self.tmp.name) / 'large.pb'
            with self.assertRaisesRegex(ValueError, 'byte limit'):
                peak.fetch(self.url + '/large', 100, destination)
            self.assertFalse(destination.exists())
            with self.assertRaisesRegex(ValueError, 'byte limit'):
                peak.fetch(self.url + '/large', 100)
        self.server.responses['/exact'] = (200, b'x' * 100, {})
        self.assertEqual(len(peak.fetch(self.url + '/exact', 100)), 100)

    def test_incomplete_http_response_is_not_saved_as_profile(self):
        self.server.responses['/truncated'] = (200, b'short', {'Content-Length': '100'})
        destination = Path(self.tmp.name) / 'truncated.pb'
        with self.assertRaisesRegex(ValueError, 'incomplete HTTP response'):
            peak.fetch(self.url + '/truncated', 100, destination)
        self.assertFalse(destination.exists())

    def test_raw_after_metrics_retained_when_diagnostics_disappear(self):
        capture = peak.Capture(self.args)
        missing = b'# exporter reset\nunrelated_metric 1\n'

        def snapshot(folder, phase):
            if phase == 'after':
                self.server.responses['/metrics'] = (200, missing, {})

        with mock.patch.object(capture, 'snapshot', side_effect=snapshot), \
                mock.patch.object(peak.shutil, 'which', return_value='/tool'), \
                mock.patch.object(peak, 'command'), redirect_stdout(self.quiet), redirect_stderr(self.quiet):
            self.assertEqual(capture.run(), 1)
        self.assertEqual((capture.root / '01-baseline/metrics-after.txt').read_bytes(), missing)
        self.assertIn('parse metrics after', (capture.root / 'errors.log').read_text())

    def test_redirect_and_proxy_protection(self):
        self.server.responses['/redirect'] = (302, b'', {'Location': self.url + '/secret'})
        with self.assertRaisesRegex(ValueError, 'redirects'):
            peak.fetch(self.url + '/redirect', 100)
        self.assertNotIn('/secret', self.server.requests)
        with mock.patch.dict(os.environ, {'http_proxy': 'http://192.0.2.1:9', 'no_proxy': ''}):
            self.assertEqual(peak.fetch(self.url + '/metrics', 4096), exposition())

    def test_stopped_http_removes_partial_profile(self):
        stop = threading.Event()
        stop.set()
        path = Path(self.tmp.name) / 'cancelled.pb'
        with self.assertRaises(TimeoutError):
            peak.fetch(self.url + '/profile', 1024, path, stop)
        self.assertFalse(path.exists())

    def test_command_output_bounded_and_cancelled(self):
        path = Path(self.tmp.name) / 'command.txt'
        with self.assertRaisesRegex(ValueError, 'byte limit'):
            peak.command([sys.executable, '-c', 'print("x" * 1000)'], path, threading.Event(), 100)
        self.assertLessEqual(path.stat().st_size, 100)
        stop = threading.Event()
        stop.set()
        with self.assertRaises(TimeoutError):
            peak.command([sys.executable, '-c', 'import time; time.sleep(30)'],
                         Path(self.tmp.name) / 'stopped.txt', stop)

    def test_repeated_metrics_failures_are_prominent_and_stop_cleanly(self):
        self.args.max_bundles = 24
        self.args.poll_interval = .001
        capture = peak.Capture(self.args)
        first = (exposition(), peak.metrics(exposition(), self.args.listener))
        calls = 0

        def scrape():
            nonlocal calls
            calls += 1
            if calls == 1:
                return first
            if calls == 4:
                capture.stop.set()
            raise OSError('offline')

        with mock.patch.object(capture, 'scrape', side_effect=scrape), \
                mock.patch.object(capture, 'bundle', return_value=first[1]), \
                redirect_stdout(self.quiet), redirect_stderr(self.quiet):
            self.assertEqual(capture.run(), 1)
        self.assertIn('METRICS FAILURE #3; capture blind', (capture.root / 'errors.log').read_text())

    def test_low_free_space_fails_before_http_or_capture(self):
        self.disk_usage.return_value.free = self.args.min_free_bytes - 1
        with mock.patch.object(peak, 'arguments', return_value=self.args), redirect_stderr(self.quiet):
            self.assertEqual(peak.main(), 1)
        self.assertEqual(self.server.requests, [])
        self.assertEqual(list(Path(self.args.output).iterdir()), [])
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)
        self.assertNotIn('FATAL', self.quiet.getvalue())

    def test_metrics_snapshot_obeys_total_budget(self):
        self.args.max_total_bytes = 1
        capture, result = self.run_capture()
        self.assertEqual(result, 1)
        self.assertTrue(capture.stop.is_set())
        self.assertEqual(capture.storage.total, 0)
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)
        self.assertEqual(self.server.requests, ['/metrics', '/debug/pprof/'])

    def test_metadata_cannot_bypass_budget_or_hide_storage_stop(self):
        self.args.max_total_bytes = 2 * len(exposition()) + 8 * len(b'profile bytes') + 50
        capture, result = self.run_capture()
        self.assertEqual(result, 1)
        self.assertTrue(capture.stop.is_set())
        self.assertTrue((capture.root / '01-baseline/cpu.pb').exists())
        self.assertTrue((capture.root / '01-baseline/metrics-after.txt').exists())
        self.assertLessEqual(capture.storage.total, self.args.max_total_bytes)
        self.assertEqual(capture.storage.total, sum(p.stat().st_size for p in capture.root.rglob('*') if p.is_file()))
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)
        self.assertNotIn('ERROR:', self.quiet.getvalue())
        self.assertNotIn(capture.root / '01-baseline', capture.storage.completed)

    def test_partial_http_files_are_removed_and_refunded(self):
        self.server.responses['/large'] = (200, b'x' * 200000, {})
        for budget, response_limit, stopped in ((70000, 300000, True), (1000000, 70000, False)):
            with self.subTest(budget=budget):
                self.args.output = self.tmp.name + '/' + str(budget)
                self.args.max_total_bytes = budget
                capture = peak.Capture(self.args)
                destination = capture.root / 'partial.pb'
                with redirect_stderr(self.quiet), self.assertRaises((peak.StorageLimit, ValueError)):
                    peak.fetch(self.url + '/large', response_limit, destination, capture.stop, capture.storage)
                self.assertFalse(destination.exists())
                self.assertEqual(capture.storage.total, 0)
                self.assertEqual(capture.storage.live, set())
                self.assertEqual(capture.stop.is_set(), stopped)

    def test_concurrent_http_profiles_share_one_budget(self):
        self.args.max_total_bytes = 500
        capture = peak.Capture(self.args)
        barrier = threading.Barrier(4)
        self.server.responses['/profile'] = (200, b'x' * 300, {})
        self.server.on_request = lambda path: barrier.wait(timeout=5)
        observed = []

        def usage(root):
            observed.append(capture.storage.total)
            return SimpleNamespace(free=100 * 1024 ** 3)

        self.disk_usage.side_effect = usage

        def download(number):
            try:
                peak.fetch(self.url + '/profile', 1000, capture.root / f'{number}.pb',
                           capture.stop, capture.storage)
            except (peak.StorageLimit, TimeoutError):
                pass

        with redirect_stderr(self.quiet), ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(download, range(4)))
        self.assertTrue(capture.stop.is_set())
        self.assertLessEqual(max(observed), 500)
        self.assertLessEqual(capture.storage.total, 500)
        self.assertEqual(capture.storage.total, sum(p.stat().st_size for p in capture.root.iterdir()))
        self.assertEqual(capture.storage.live, set())
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)


class StorageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        usage = mock.patch.object(peak.shutil, 'disk_usage', return_value=SimpleNamespace(free=100 * 1024 ** 3))
        self.disk_usage = usage.start()
        self.addCleanup(usage.stop)
        self.args = peak.arguments(['--output', self.tmp.name + '/run'])
        self.capture = peak.Capture(self.args)
        self.storage = self.capture.storage
        self.quiet = io.StringIO()

    def disk_bytes(self):
        return sum(p.stat().st_size for p in self.capture.root.rglob('*') if p.is_file())

    def test_concurrent_writers_cannot_overshoot_budget_and_wake_waiters(self):
        self.args.max_total_bytes = 1000
        barrier = threading.Barrier(4)
        outputs = [self.storage.open(self.capture.root / f'{i}.pb') for i in range(4)]

        def write(output):
            with output:
                barrier.wait(timeout=5)
                try:
                    for _ in range(20):
                        output.write(b'x' * 100)
                except peak.StorageLimit:
                    pass

        with redirect_stderr(self.quiet), ThreadPoolExecutor(max_workers=5) as pool:
            waiter = pool.submit(self.capture.stop.wait, 5)
            list(pool.map(write, outputs))
            self.assertTrue(waiter.result())
        self.assertEqual(self.storage.total, 1000)
        self.assertEqual(self.disk_bytes(), 1000)
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)
        self.assertEqual(self.storage.live, set())

    def test_free_space_checked_under_lock_for_each_chunk(self):
        self.args.min_free_bytes = 200
        self.disk_usage.side_effect = lambda root: SimpleNamespace(free=1200 - self.storage.total)
        with redirect_stderr(self.quiet), self.storage.open(self.capture.root / 'sample') as output:
            for _ in range(10):
                output.write(b'x' * 100)
            with self.assertRaises(peak.StorageLimit):
                output.write(b'x')
        self.assertEqual(self.disk_bytes(), 1000)
        self.assertGreaterEqual(self.disk_usage.call_count, 12)
        self.assertTrue(self.capture.stop.is_set())

    def test_runtime_free_space_decline_stops_future_output(self):
        path = self.capture.root / 'useful.txt'
        with self.storage.open(path) as output:
            output.write(b'keep this')
            self.disk_usage.return_value.free = self.args.min_free_bytes - 1
            with redirect_stderr(self.quiet), self.assertRaises(peak.StorageLimit):
                output.write(b'no')
        with redirect_stderr(self.quiet):
            self.capture.error('must not write errors.log')
        self.assertEqual(path.read_bytes(), b'keep this')
        self.assertFalse((self.capture.root / 'errors.log').exists())
        self.assertEqual(self.storage.total, len(b'keep this'))
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)

    def test_actual_write_failure_stops_without_losing_previous_bytes(self):
        path = self.capture.root / 'sample'
        with self.storage.open(path) as output:
            output.write(b'useful')
            with mock.patch.object(output.output, 'write', side_effect=OSError('No space left on device')), \
                    redirect_stderr(self.quiet), self.assertRaises(peak.StorageLimit):
                output.write(b'failed')
        self.assertEqual(path.read_bytes(), b'useful')
        self.assertEqual(self.storage.total, len(b'useful'))
        self.assertTrue(self.capture.stop.is_set())
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)

    def test_errors_log_obeys_budget_and_is_silent_after_limit(self):
        self.args.max_total_bytes = 100
        with redirect_stderr(self.quiet):
            self.capture.error('first')
            self.capture.error('x' * 200)
            after_stop = self.quiet.getvalue()
            for _ in range(20):
                self.capture.error('retry')
        self.assertEqual(self.quiet.getvalue(), after_stop)
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)
        self.assertIn('first', (self.capture.root / 'errors.log').read_text())
        self.assertNotIn('retry', (self.capture.root / 'errors.log').read_text())
        self.assertEqual(self.storage.total, self.disk_bytes())
        self.assertLessEqual(self.disk_bytes(), 100)

    def test_proc_reads_use_shared_budget(self):
        self.args.max_total_bytes = 100
        folder = self.storage.new_bundle(1, 'baseline')
        with mock.patch('builtins.open', side_effect=lambda *a, **kw: io.BytesIO(b'x' * 80)), \
                redirect_stderr(self.quiet):
            self.capture.proc(folder)
        self.assertEqual(self.disk_bytes(), 80)
        self.assertEqual(self.storage.total, 80)
        self.assertTrue(self.capture.stop.is_set())
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)

    def test_thread_schedstats_use_shared_budget(self):
        self.args.pid, self.args.thread_schedstats = 123, True
        self.args.max_total_bytes = 5
        folder = self.storage.new_bundle(1, 'baseline')
        entries = mock.MagicMock()
        entries.__enter__.return_value = [SimpleNamespace(path='/proc/123/task/123', name='123')]

        def source(path, *args):
            return io.StringIO('1 2 3') if '/task/' in path else io.BytesIO(b'')

        with mock.patch('builtins.open', side_effect=source), mock.patch.object(peak.os, 'scandir', return_value=entries), \
                redirect_stderr(self.quiet):
            self.capture.proc(folder)
        self.assertTrue(self.capture.stop.is_set())
        self.assertEqual(self.storage.total, 0)
        self.assertEqual(self.quiet.getvalue().count('STORAGE STOP:'), 1)

    def test_subprocess_output_uses_shared_budget(self):
        self.args.max_total_bytes = 10
        destination = self.capture.root / 'command.txt'
        with redirect_stderr(self.quiet), self.assertRaises(peak.StorageLimit):
            peak.command([sys.executable, '-c', 'print("x" * 100)'], destination,
                         self.capture.stop, storage=self.storage)
        self.assertTrue(self.capture.stop.is_set())
        self.assertLessEqual(destination.stat().st_size, 10)
        self.assertEqual(self.storage.total, self.disk_bytes())
        self.assertEqual(self.storage.live, set())

    def fill_bundle(self, number, reason):
        folder = self.storage.new_bundle(number, reason)
        child = folder / 'before'
        self.storage.mkdir(child)
        self.storage.save(child / 'data.txt', str(number).encode() * number)
        self.storage.completed.add(folder)
        return folder

    def test_retention_pins_first_baseline_and_first_peak_and_refunds_bytes(self):
        outside = Path(self.tmp.name) / 'unrelated'
        outside.write_bytes(b'leave alone')
        foreign = self.capture.root / 'unrelated'
        foreign.mkdir()
        (foreign / 'data').write_bytes(b'leave alone')
        created = []
        for number in range(1, 11):
            reason = 'baseline' if number == 1 else ('queue' if number in (3, 6) else 'periodic-baseline')
            created.append(self.fill_bundle(number, reason))
            self.assertLessEqual(len(self.storage.bundles), 6)
            expected_bytes = sum(p.stat().st_size for folder in self.storage.bundles
                                 for p in folder.rglob('*') if p.is_file())
            self.assertEqual(self.storage.total, expected_bytes)
        self.assertEqual(self.storage.bundles, [created[i - 1] for i in (1, 3, 7, 8, 9, 10)])
        self.assertEqual(self.storage.pinned, {created[0], created[2]})
        self.assertFalse(created[1].exists())
        self.assertEqual(outside.read_bytes(), b'leave alone')
        self.assertEqual((foreign / 'data').read_bytes(), b'leave alone')

    def test_no_peak_retains_baseline_and_five_recent_bundles(self):
        created = [self.fill_bundle(i, 'baseline' if i == 1 else 'periodic-baseline') for i in range(1, 10)]
        self.assertEqual(self.storage.bundles, [created[0], *created[4:]])
        self.assertEqual(self.storage.total, self.disk_bytes())
        self.assertEqual(self.storage.pinned, {created[0]})

    def test_retention_never_deletes_incomplete_or_open_bundles(self):
        self.args.keep_bundles = 3
        baseline = self.fill_bundle(1, 'baseline')
        active = self.storage.new_bundle(2, 'periodic-baseline')
        completed = self.fill_bundle(3, 'periodic-baseline')
        with self.storage.open(active / 'in-flight.pb') as output:
            output.write(b'useful')
            new = self.storage.new_bundle(4, 'queue')
            self.assertEqual(self.storage.bundles, [baseline, active, new])
            self.assertFalse(completed.exists())
            self.assertEqual((active / 'in-flight.pb').read_bytes(), b'useful')
        self.assertEqual(self.storage.total, self.disk_bytes())

    def test_retention_rejects_unowned_file_without_deleting_evidence(self):
        self.args.keep_bundles = 3
        self.fill_bundle(1, 'baseline')
        victim = self.fill_bundle(2, 'periodic-baseline')
        self.fill_bundle(3, 'drops')
        foreign = victim / 'foreign'
        foreign.write_bytes(b'not ours')
        with redirect_stderr(self.quiet), self.assertRaises(peak.StorageLimit):
            self.storage.new_bundle(4, 'periodic-baseline')
        self.assertEqual(foreign.read_bytes(), b'not ours')
        self.assertTrue((victim / 'before/data.txt').exists())

    def test_retention_rejects_replaced_bundle_symlink(self):
        self.args.keep_bundles = 3
        self.fill_bundle(1, 'baseline')
        victim = self.fill_bundle(2, 'periodic-baseline')
        self.fill_bundle(3, 'drops')
        moved = Path(self.tmp.name) / 'moved'
        victim.rename(moved)
        victim.symlink_to(moved, target_is_directory=True)
        with redirect_stderr(self.quiet), self.assertRaises(peak.StorageLimit):
            self.storage.new_bundle(4, 'periodic-baseline')
        self.assertEqual((moved / 'before/data.txt').read_bytes(), b'22')
        self.assertTrue(victim.is_symlink())


class ArgumentsTest(unittest.TestCase):
    def test_literal_loopback_only(self):
        for url in ('http://127.0.0.1:8081/metrics', 'http://[::1]:6060/debug/pprof'):
            self.assertEqual(peak.loopback_url(url), url)
        for url in ('http://localhost:8081', 'http://192.0.2.1:8081', 'https://127.0.0.1:8081',
                    'http://user:secret@127.0.0.1:8081', 'http://127.0.0.1:8081/?token=x',
                    'http://127.0.0.1:8081/#x', 'http://127.1:8081', 'file:///proc/1/environ'):
            with self.subTest(url=url), self.assertRaises(argparse.ArgumentTypeError):
                peak.loopback_url(url)

    def test_defaults_and_invalid_limits(self):
        args = peak.arguments([])
        self.assertEqual((args.duration, args.cooldown, args.baseline_interval, args.max_bundles,
                          args.trace_seconds), (86400, 900, 3600, 24, 0))
        self.assertEqual((args.max_total_bytes, args.min_free_bytes, args.keep_bundles, args.profile_bytes),
                         (2147483648, 5368709120, 6, 16 * peak.MIB))
        for argv in (['--trace-seconds', '-1'], ['--trace-seconds', '6'], ['--trace-seconds', '0.5'],
                     ['--max-bundles', '25'], ['--max-bundles', '0'], ['--workers', '9'],
                     ['--duration', '0'], ['--pid', '0'], ['--thread-schedstats'],
                     ['--keep-bundles', '2'], ['--keep-bundles', '-1'], ['--max-total-bytes', '0'],
                     ['--min-free-bytes', '-1'],
                     ['--profile-bytes', str(128 * peak.MIB + 1)],
                     ['--metric-bytes', str(16 * peak.MIB + 1)]):
            with self.subTest(argv=argv), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                peak.arguments(argv)


if __name__ == '__main__':
    unittest.main()
