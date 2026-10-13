"""Synthetic archive/cache controls; no network, Cargo or native execution."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock
import zipfile

spec = importlib.util.spec_from_file_location('linux_static_ort', Path(__file__).with_name('linux_static_ort.py'))
linux = importlib.util.module_from_spec(spec)
spec.loader.exec_module(linux)


def fixture(directory, *, header=b'#define ORT_API_VERSION 24\n', extra=None):
    lock = json.loads(linux.LOCK.read_text())
    lock.pop('notices', None)  # Synthetic fixture does not package upstream legal texts.
    members = {'lib/libonnxruntime.a': b'!<arch>\nsynthetic, never linked\n',
               'include/onnxruntime_c_api.h': header}
    lock['files'] = {name: {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
                     for name, raw in members.items()}
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as output:
        for name, raw in members.items():
            member = zipfile.ZipInfo(lock['archive']['root'] + '/' + name)
            member.external_attr = (stat.S_IFREG | 0o644) << 16
            output.writestr(member, raw)
        if extra:
            extra(output, lock)
    raw = archive.getvalue()
    lock['archive'].update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    path = directory / 'linux-static-ort.lock.json'
    path.write_text(json.dumps(lock))
    return lock, path, raw


class LinuxStaticOrtTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name).resolve()
        self.lock, self.lock_path, self.raw = fixture(self.home)
        self.download = mock.patch.object(linux, 'download', side_effect=self.fetch).start()
        self.addCleanup(mock.patch.stopall)

    def fetch(self, archive, destination):
        destination.write_bytes(self.raw)

    def prepare(self):
        return linux.prepare(self.home, self.lock_path)

    def test_pinned_cache_reuse_does_not_download_twice(self):
        path = self.prepare()
        self.assertEqual(self.prepare(), path)
        self.download.assert_called_once()
        proof = linux.verify(self.home, self.lock_path)
        self.assertEqual(proof['archive']['sha256'], self.lock['archive']['sha256'])
        self.assertNotEqual(proof['archive']['sha256'], proof['members']['lib/libonnxruntime.a']['sha256'])

    def test_corrupt_cached_library_refuses_without_repair_or_download(self):
        path = self.prepare() / 'libonnxruntime.a'
        path.write_bytes(b'!<arch>\ncorrupt\n')
        with self.assertRaisesRegex(ValueError, 'checksum or size differs'):
            self.prepare()
        self.download.assert_called_once()
        self.assertEqual(path.read_bytes(), b'!<arch>\ncorrupt\n')

    def test_corrupt_cached_download_refused_even_with_valid_library(self):
        self.prepare()
        (linux.cache_path(self.home, self.lock) / 'upstream.zip').write_bytes(b'wrong')
        with self.assertRaisesRegex(ValueError, 'checksum or size differs'):
            self.prepare()

    def test_symlink_and_hardlink_library_refused(self):
        import os
        path = self.prepare() / 'libonnxruntime.a'
        other = self.home / 'other.a'
        path.rename(other)
        for link in ('symlink', 'hardlink'):
            with self.subTest(link=link):
                path.symlink_to(other) if link == 'symlink' else os.link(other, path)
                with self.assertRaises(ValueError):
                    self.prepare()
                path.unlink()

    def test_unlisted_cache_file_refused(self):
        (self.prepare() / 'unlisted.so').write_bytes(b'other')
        with self.assertRaisesRegex(ValueError, 'inventory differs'):
            self.prepare()

    def test_wrong_api_header_refused_even_when_hash_matches(self):
        self.lock, self.lock_path, self.raw = fixture(self.home, header=b'#define ORT_API_VERSION 23\n')
        with self.assertRaisesRegex(ValueError, 'API 24'):
            self.prepare()

    def test_unsafe_or_unexpected_zip_member_refused_before_extract(self):
        for name in ('../escaped', '/absolute', 'other/unlisted.so'):
            with self.subTest(name=name):
                self.lock, self.lock_path, self.raw = fixture(
                    self.home, extra=lambda output, lock: output.writestr(name, b'bad'))
                with self.assertRaisesRegex(ValueError, 'inventory differs'):
                    self.prepare()
                self.assertFalse(linux.cache_path(self.home, self.lock).exists())
        self.assertFalse((self.home.parent / 'escaped').exists())

    def test_zip_symlink_replacing_expected_library_refused(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(self.raw)) as original, zipfile.ZipFile(buffer, 'w') as output:
            for member in original.infolist():
                raw = original.read(member)
                if member.filename.endswith('/libonnxruntime.a'):
                    member.external_attr = (stat.S_IFLNK | 0o777) << 16
                output.writestr(member, raw)
        self.raw = buffer.getvalue()
        self.lock['archive'].update(bytes=len(self.raw), sha256=hashlib.sha256(self.raw).hexdigest())
        self.lock_path.write_text(json.dumps(self.lock))
        with self.assertRaisesRegex(ValueError, 'regular member'):
            self.prepare()

    def test_download_checksum_truncation_and_oversize_refused(self):
        # Exercise the actual downloader with a local in-memory HTTPS response.
        mock.patch.stopall()
        for raw in (self.raw[:-1], self.raw + b'x', b'x' * len(self.raw)):
            response = io.BytesIO(raw)
            response.url = 'https://release-assets.githubusercontent.com/fixture'
            with self.subTest(size=len(raw)), mock.patch.object(linux.urllib.request, 'urlopen', return_value=response):
                with tempfile.TemporaryDirectory(dir=self.home) as directory:
                    with self.assertRaisesRegex(ValueError, 'checksum|size'):
                        linux.download(self.lock['archive'], Path(directory) / 'download.zip')

    def test_http_redirect_refused(self):
        mock.patch.stopall()
        response = io.BytesIO(self.raw)
        response.url = 'http://example.invalid/fixture'
        with mock.patch.object(linux.urllib.request, 'urlopen', return_value=response):
            with self.assertRaisesRegex(ValueError, 'HTTPS'):
                linux.download(self.lock['archive'], self.home / 'bad.zip')


if __name__ == '__main__':
    unittest.main()
