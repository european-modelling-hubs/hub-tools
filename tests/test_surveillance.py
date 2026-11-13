"""Offline tests: HTTP outcomes, import compatibility, and persistence retries."""
import contextlib
import csv
from datetime import date
import importlib.util
import io
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

CODE = Path(__file__).resolve().parents[1] / 'code'
spec = importlib.util.spec_from_file_location('check_release', CODE / 'check_erviss_release.py')
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def response(self, status=200):
        response = Mock(status=status)
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        return response

    def http_error(self, status):
        return HTTPError('https://example.test/release.csv', status, 'test', {}, None)

    def test_last_friday_across_midnight_weekend_monday_and_year(self):
        for today, expected in [
            ('2026-11-13', '2026-11-13'), ('2026-11-14', '2026-11-13'),
            ('2026-11-15', '2026-11-13'), ('2026-11-16', '2026-11-13'),
            ('2026-11-19', '2026-11-13'), ('2026-11-20', '2026-11-20'),
            ('2027-01-01', '2027-01-01'), ('2026-01-01', '2025-12-26'),
        ]:
            with self.subTest(today=today):
                self.assertEqual(check.get_snapshot_date(date.fromisoformat(today)), expected)

    def test_marker_precedes_network_and_partial_import_is_not_completion(self):
        snapshot = '2026-11-13'
        imported = self.root / 'target-data/ERVISS/snapshots'
        imported.mkdir(parents=True)
        (imported / f'{snapshot}-ILI_incidence.csv').touch()
        with patch.object(check, 'urlopen', return_value=self.response()) as request:
            self.assertEqual(check.check_release(self.root, snapshot), 'AVAILABLE')
            self.assertEqual(request.call_args.args[0].method, 'HEAD')
        marker = self.root / check.MARKER_DIR / snapshot
        marker.parent.mkdir(parents=True)
        marker.touch()
        with patch.object(check, 'urlopen') as request:
            self.assertEqual(check.check_release(self.root, snapshot), 'ALREADY_IMPORTED')
            request.assert_not_called()

    def test_http_outcomes_and_head_fallback(self):
        for status, expected in [(200, 'AVAILABLE'), (404, 'NOT_AVAILABLE')]:
            effect = self.response(status) if status == 200 else None
            with patch.object(check, 'urlopen', return_value=effect,
                              side_effect=self.http_error(status) if status == 404 else None):
                self.assertEqual(check.check_release(self.root, '2026-11-13'), expected)
        for head_status in (405, 501):
            for fallback, expected in [(self.response(206), 'AVAILABLE'),
                                       (self.http_error(404), 'NOT_AVAILABLE')]:
                with self.subTest(head=head_status, fallback=expected):
                    with patch.object(check, 'urlopen', side_effect=[self.http_error(head_status), fallback]) as request:
                        self.assertEqual(check.check_release(self.root, '2026-11-13'), expected)
                        self.assertEqual(request.call_args.args[0].get_method(), 'GET')
        for error in [self.http_error(403), self.http_error(429), self.http_error(500),
                      URLError('network failure'), TimeoutError('timeout')]:
            with patch.object(check, 'urlopen', side_effect=error):
                with self.assertRaises((OSError, URLError)):
                    check.check_release(self.root, '2026-11-13')
        with patch.object(check, 'urlopen', side_effect=[self.http_error(405), self.http_error(503)]):
            with self.assertRaises(HTTPError):
                check.check_release(self.root, '2026-11-13')

    def test_cli_outputs_and_exit_codes(self):
        output = self.root / 'github-output'
        for status in ('AVAILABLE', 'ALREADY_IMPORTED', 'NOT_AVAILABLE', 'ERROR'):
            with self.subTest(status=status):
                output.write_text('previous=value\n')
                effect = OSError('technical failure') if status == 'ERROR' else None
                with patch.object(check, 'check_release', return_value=status, side_effect=effect), \
                     patch.object(check, 'get_snapshot_date', return_value='2026-11-13'), \
                     patch.dict(os.environ, {'GITHUB_OUTPUT': str(output)}), \
                     contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    result = check.main(['--hub_path', str(self.root)])
                self.assertEqual(result, 1 if status == 'ERROR' else 0)
                self.assertEqual(output.read_text(), f'previous=value\nstatus={status}\nsnapshot_date=2026-11-13\n')
        with patch.object(check, 'check_release', return_value='AVAILABLE'), \
             patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(check.main([]), 0)

    def test_marker_storage_errors_are_technical_errors(self):
        marker = self.root / check.MARKER_DIR / '2026-11-13'
        marker.mkdir(parents=True)
        with self.assertRaises(OSError):
            check.check_release(self.root, '2026-11-13')
        with patch.object(check.Path, 'stat', side_effect=PermissionError('denied')):
            with self.assertRaises(PermissionError):
                check.check_release(self.root, '2026-11-13')


class ImportAndPersistenceTests(unittest.TestCase):
    def test_import_explicit_date_and_legacy_invocation(self):
        import pycountry  # Exercise real dependencies and original transformations.
        import isoweek
        data = ('indicator,survtype,age,countryname,yearweek,value\n'
                'ILI,primary care syndromic,total,Malta,2026-W45,1.5\n'
                'ARI,primary care syndromic,total,Italy,2026-W45,2\n'
                'ILI,primary care syndromic,0-4,Italy,2026-W45,999\n').encode()
        for explicit in (True, False):
            with self.subTest(explicit=explicit), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                (root / 'target-data/ERVISS/snapshots').mkdir(parents=True)
                output = root / 'github-output'
                argv = ['import_truth_ILI_ARI.py', '--hub_path', temp]
                if explicit:
                    argv += ['--snapshot_date', '2026-11-13']
                response = Mock()
                response.readlines.return_value = data.splitlines(keepends=True)
                with patch.object(sys, 'argv', argv), \
                     patch('urllib.request.urlopen', return_value=response) as request, \
                     patch.dict(os.environ, {'GITHUB_OUTPUT': str(output)}), \
                     contextlib.redirect_stdout(io.StringIO()):
                    result = runpy.run_path(str(CODE / 'import_truth_ILI_ARI.py'))
                snapshot = '2026-11-13' if explicit else result['get_snapshot_date']()
                self.assertIn(snapshot + '_ILIARIRates.csv', request.call_args.args[0])
                ili = root / f'target-data/ERVISS/snapshots/{snapshot}-ILI_incidence.csv'
                with ili.open() as stream:
                    rows = list(csv.DictReader(stream))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['location'], 'MT')
                self.assertEqual(rows[0]['truth_date'], '2026-11-08')
                self.assertEqual(float(rows[0]['value']), 1500)
                self.assertEqual(ili.read_bytes(), (root / 'target-data/ERVISS/latest-ILI_incidence.csv').read_bytes())
                self.assertIn(f'{snapshot}-ARI_incidence.csv', output.read_text())
                self.assertTrue(output.read_text().endswith('\n'))

    def test_invalid_date_rejected_before_network(self):
        for value in ('2026-11-14', '../marker', '2026-02-30', '2026-1-2'):
            with self.subTest(value=value), patch.object(sys, 'argv', ['import', '--snapshot_date', value]), \
                 patch('urllib.request.urlopen') as request, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    runpy.run_path(str(CODE / 'import_truth_ILI_ARI.py'))
                self.assertEqual(error.exception.code, 2)
                request.assert_not_called()

    def test_persistence_same_target_multiple_sources_and_retry_then_merge(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / 'repo/.github/data-storage/target_db.json'
            db.parent.mkdir(parents=True)
            db.write_text('{}')
            files = [f'target-data/{source}/snapshots/2026-11-13-{target}_incidence.csv'
                     for source in ['ERVISS', 'FluID'] for target in ['ILI', 'ARI']]
            old = Path.cwd()
            try:
                os.chdir(root)
                with patch.dict(os.environ, {'data': ' '.join(files)}), contextlib.redirect_stdout(io.StringIO()):
                    for _ in range(2):
                        runpy.run_path(str(CODE / 'persist_changes.py'), run_name='__main__')
                persisted = json.loads(db.read_text())
                for target in ['ILI', 'ARI']:
                    self.assertEqual(set(persisted[f'{target} incidence']['changes']),
                                     {f for f in files if f'{target}_incidence' in f})
                for source in ['ERVISS', 'FluID']:
                    folder = root / f'repo/target-data/{source}'
                    folder.mkdir(parents=True)
                    for target in ['ILI', 'ARI']:
                        (folder / f'latest-{target}_incidence.csv').write_text(
                            f'target,location,truth_date,year_week,value\n{target} incidence,IT,2026-11-08,2026-W45,2\n')
                with patch.object(sys, 'argv', ['merge', '--source_list', 'ERVISS FluID']), \
                     patch.dict(os.environ, {'disease_name': 'Syndromic_indicator'}), \
                     contextlib.redirect_stdout(io.StringIO()):
                    runpy.run_path(str(CODE / 'merge_recent_data_ILI_ARI.py'))
                for target in ['ILI', 'ARI']:
                    with (root / f'repo/target-data/latest-{target}_incidence.csv').open() as stream:
                        rows = list(csv.DictReader(stream))
                    self.assertEqual([r['data_source'] for r in rows], ['ERVISS', 'FluID'])
            finally:
                os.chdir(old)


if __name__ == '__main__':
    unittest.main()
