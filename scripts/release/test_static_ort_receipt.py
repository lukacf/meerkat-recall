"""Synthetic file controls: no Cargo, provider, process or runtime execution."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('static_ort_receipt', Path(__file__).with_name('static_ort_receipt.py'))
receipt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(receipt)


class StaticOrtReceiptTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.home = self.root / 'cargo'
        self.deploy = self.root / 'target/deploy'
        self.fingerprints = self.deploy / '.fingerprint'
        (self.deploy / 'deps').mkdir(parents=True)
        self.binary = self.deploy / 'meerkat-recall'
        self.binary.write_bytes(b'fixture executable; never run')
        os.link(self.binary, self.deploy / 'deps/meerkat_recall-0000000000000001')
        self.graph = {}
        units = [
            ('meerkat-recall-bin', 'bin-meerkat-recall', 1, 'elephant_embeddings', 2),
            ('elephant-embeddings', 'lib-elephant_embeddings', 2, 'fastembed', 3),
            ('fastembed', 'lib-fastembed', 3, 'ort', 4),
            ('ort', 'lib-ort', 4, 'ort_sys', 5),
            ('ort-sys', 'lib-ort_sys', 5, 'build_script_main', 6),
            ('ort-sys', 'run-build-script-build-script-main', 6, None, None),
        ]
        for crate, filename, number, dep, dep_number in units:
            features = ['default', 'rocksdb', 'fastembed'] if number == 1 else ['api-24', 'download-binaries', 'std']
            value = {'features': json.dumps(features), 'deps': [[123, dep, False, dep_number]] if dep else []}
            self.graph[filename] = self.unit(crate, filename, number, value)
        self.target = 'aarch64-apple-darwin'
        self.dist_hash = 'd' * 64
        self.library = self.root / 'dfbin' / self.target / self.dist_hash / 'libonnxruntime.a'
        self.library.parent.mkdir(parents=True)
        self.library.write_bytes(b'!<arch>\nfixture only\n')
        self.output = self.deploy / 'build/ort-sys-0000000000000006/output'
        self.output.parent.mkdir(parents=True)
        self.output.write_text(f'cargo:rustc-link-search=native={self.library.parent}\ncargo:rustc-link-lib=static=onnxruntime\n')
        self.source = self.home / 'registry/src/registry-test/ort-sys-2.0.0-rc.12'
        self.table = self.source / 'build/download/dist.txt'
        self.table.parent.mkdir(parents=True)
        self.table.write_text(f'none\t{self.target}\thttps://example.invalid/runtime.tar.lzma2\t{self.dist_hash}\n')
        self.package = self.home / 'registry/cache/registry-test/ort-sys-2.0.0-rc.12.crate'
        self.package.parent.mkdir(parents=True)
        with tarfile.open(self.package, 'w:gz') as archive:
            raw = self.table.read_bytes()
            info = tarfile.TarInfo('ort-sys-2.0.0-rc.12/build/download/dist.txt')
            info.size = len(raw)
            archive.addfile(info, io.BytesIO(raw))
        package_hash = hashlib.sha256(self.package.read_bytes()).hexdigest()
        lock = []
        for name, version, checksum in [('fastembed', '5.17.2', 'f' * 64),
                                         ('ort', '2.0.0-rc.12', 'a' * 64),
                                         ('ort-sys', '2.0.0-rc.12', package_hash)]:
            lock.append(f'[[package]]\nname="{name}"\nversion="{version}"\nsource="registry+https://github.com/rust-lang/crates.io-index"\nchecksum="{checksum}"\n')
        (self.root / 'Cargo.lock').write_text('\n'.join(lock))
        self.environment = self.root / 'cargo-env'
        self.environment.write_text(f'CARGO_HOME={self.home}\nCARGO_TARGET_DIR={self.root / "target"}\n')

    def unit(self, crate, filename, number, value):
        path = self.fingerprints / f'{crate}-{number:016x}' / (filename + '.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))
        path.with_suffix('').write_text(number.to_bytes(8, 'little').hex())
        return path

    def collect(self):
        return receipt.collect(self.root, self.environment, self.target)

    def test_retains_selected_raw_fingerprints_link_and_distinct_archive_digest(self):
        value = self.collect()
        self.assertEqual(value['features']['ort'], ['api-24', 'download-binaries', 'std'])
        self.assertEqual(len(value['dependency_fingerprints']), 6)
        self.assertEqual(value['link_output']['text'], self.output.read_text())
        self.assertEqual(value['distribution']['sha256'], self.dist_hash)
        self.assertEqual(value['static_library']['sha256'], hashlib.sha256(self.library.read_bytes()).hexdigest())
        self.assertNotEqual(value['static_library']['sha256'], value['distribution']['sha256'])
        for item in value['dependency_fingerprints']:
            self.assertEqual(hashlib.sha256(item['text'].encode()).hexdigest(), item['sha256'])
        self.assertEqual(value['binary_sha256'], hashlib.sha256(self.binary.read_bytes()).hexdigest())

    def test_old_unselected_feature_instance_does_not_replace_dependency(self):
        self.unit('ort', 'lib-ort', 7, {'features': '["load-dynamic"]', 'deps': []})
        self.assertFalse(self.collect()['load_dynamic'])

    def test_missing_selected_dependency_refused_even_with_other_cached_instance(self):
        self.graph['lib-ort'].with_suffix('').write_text((99).to_bytes(8, 'little').hex())
        with self.assertRaisesRegex(ValueError, 'selected ort fingerprint'):
            self.collect()

    def test_duplicate_matching_fingerprint_is_ambiguous(self):
        original = self.graph['lib-ort']
        other = self.unit('ort', 'lib-ort', 7, json.loads(original.read_text()))
        other.with_suffix('').write_text(original.with_suffix('').read_text())
        with self.assertRaisesRegex(ValueError, 'selected ort fingerprint'):
            self.collect()

    def test_dynamic_or_older_api_selected_features_refused(self):
        path = self.graph['lib-ort']
        original = json.loads(path.read_text())
        for features in (['api-24', 'download-binaries', 'load-dynamic'], ['api-20', 'download-binaries']):
            path.write_text(json.dumps({**original, 'features': json.dumps(features)}))
            with self.subTest(features=features), self.assertRaises(ValueError):
                self.collect()

    def test_missing_or_ambiguous_static_link_directive_refused(self):
        original = self.output.read_text()
        for value in (original.replace('static=onnxruntime', 'onnxruntime'),
                      original + 'cargo:rustc-link-lib=dylib=onnxruntime\n'):
            self.output.write_text(value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.collect()

    def test_library_outside_selected_target_distribution_refused(self):
        wrong = self.root / 'unbound'
        wrong.mkdir()
        shutil.copyfile(self.library, wrong / self.library.name)
        self.output.write_text(f'cargo:rustc-link-search=native={wrong}\ncargo:rustc-link-lib=static=onnxruntime\n')
        with self.assertRaisesRegex(ValueError, 'checksum-addressed'):
            self.collect()

    def test_non_archive_or_symlink_library_refused(self):
        self.library.write_bytes(b'not a static library')
        with self.assertRaisesRegex(ValueError, 'static archive'):
            self.collect()
        alternate = self.library.with_name('other.a')
        self.library.rename(alternate)
        self.library.symlink_to(alternate)
        with self.assertRaisesRegex(ValueError, 'regular build input'):
            self.collect()

    def test_distribution_table_tamper_refused_against_locked_crate(self):
        self.table.write_text(self.table.read_text().replace('example.invalid', 'changed.invalid'))
        with self.assertRaisesRegex(ValueError, 'distribution table differs'):
            self.collect()

    def test_registry_archive_tamper_refused_against_cargo_lock(self):
        with self.package.open('ab') as stream:
            stream.write(b'tampered')
        with self.assertRaisesRegex(ValueError, 'checksum differs from lock'):
            self.collect()

    def test_copied_executable_selects_exact_fingerprint_and_bytes(self):
        selected = self.deploy / 'deps/meerkat_recall-0000000000000001'
        selected.unlink()
        shutil.copyfile(self.binary, selected)
        self.assertFalse(selected.samefile(self.binary))
        value = self.collect()
        self.assertEqual(value['binary_sha256'], hashlib.sha256(self.binary.read_bytes()).hexdigest())
        self.assertEqual(Path(value['dependency_fingerprints'][0]['path']).parent.name,
                         'meerkat-recall-bin-0000000000000001')

    def test_copied_executable_with_different_same_size_bytes_is_refused(self):
        selected = self.deploy / 'deps/meerkat_recall-0000000000000001'
        selected.unlink()
        selected.write_bytes(b'x' * self.binary.stat().st_size)
        with self.assertRaisesRegex(ValueError, 'selected deploy executable, found 0'):
            self.collect()

    def test_unselected_same_size_unit_and_nonexecutable_names_are_ignored(self):
        selected = self.deploy / 'deps/meerkat_recall-0000000000000001'
        selected.unlink()
        shutil.copyfile(self.binary, selected)
        (self.deploy / 'deps/meerkat_recall-0000000000000002').write_bytes(
            b'x' * self.binary.stat().st_size)
        shutil.copyfile(self.binary, self.deploy / 'deps/meerkat_recall-0000000000000001.d')
        self.assertEqual(Path(self.collect()['dependency_fingerprints'][0]['path']).parent.name,
                         'meerkat-recall-bin-0000000000000001')

    def test_matching_copied_or_hardlinked_units_remain_ambiguous(self):
        other = self.deploy / 'deps/meerkat_recall-0000000000000002'
        for copy in (False, True):
            with self.subTest(copy=copy):
                if copy:
                    shutil.copyfile(self.binary, other)
                else:
                    os.link(self.binary, other)
                with self.assertRaisesRegex(ValueError, 'selected deploy executable, found 2'):
                    self.collect()
                other.unlink()

    def test_symlinked_deps_executable_cannot_select_a_fingerprint(self):
        selected = self.deploy / 'deps/meerkat_recall-0000000000000001'
        selected.unlink()
        selected.symlink_to(self.binary)
        with self.assertRaisesRegex(ValueError, 'selected deploy executable, found 0'):
            self.collect()

    def test_duplicate_or_relative_cargo_environment_refused(self):
        original = self.environment.read_text()
        for value in (original + f'CARGO_HOME={self.home}\n', original.replace(str(self.home), 'relative')):
            self.environment.write_text(value)
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'Cargo environment'):
                self.collect()

    def linux_fixture(self):
        spec = importlib.util.spec_from_file_location(
            'linux_test_fixture', Path(__file__).with_name('test_linux_static_ort.py'))
        fixture_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture_module)
        directory = self.root / 'scripts/release'
        directory.mkdir(parents=True)
        lock, lock_path, raw = fixture_module.fixture(directory)
        self.target = 'x86_64-unknown-linux-gnu'
        self.home.mkdir(parents=True, exist_ok=True)
        cache = fixture_module.linux.cache_path(self.home, lock)
        cache.mkdir(parents=True)
        (cache / 'upstream.zip').write_bytes(raw)
        fixture_module.linux.unpack(cache / 'upstream.zip', cache / 'distribution', lock)
        self.library = cache / 'distribution/lib/libonnxruntime.a'
        self.output.write_text(f'cargo:rustc-link-search=native={self.library.parent}\ncargo:rustc-link-lib=static=onnxruntime\n')
        binary = self.graph['bin-meerkat-recall']
        value = json.loads(binary.read_text())
        value['deps'].append([123, 'build_script_build', False, 8])
        binary.write_text(json.dumps(value))
        gcc_unit = self.unit('meerkat-recall-bin', 'run-build-script-build-script-build', 8,
                             {'features': '[]', 'deps': []})
        self.gcc_archive = self.home.resolve() / 'libgcc.a'
        self.gcc_archive.write_bytes(b'!<arch>\nsynthetic compiler runtime\n')
        self.gcc_output = self.deploy / 'build' / gcc_unit.parent.name / 'output'
        self.gcc_output.parent.mkdir(parents=True)
        self.gcc_output.write_text(f'cargo:rerun-if-changed={self.gcc_archive}\ncargo:rustc-link-arg={self.gcc_archive}\n')
        return lock, lock_path

    def test_linux_receipt_binds_pinned_asset_not_unused_registry_distribution(self):
        lock, _ = self.linux_fixture()
        value = self.collect()
        self.assertEqual(value['distribution']['sha256'], lock['archive']['sha256'])
        self.assertEqual(value['distribution']['origin'], 'repository-pinned-linux-static-distribution')
        self.assertEqual(value['distribution']['members']['lib/libonnxruntime.a'], value['static_library'])
        self.assertIn('ort_sys_distribution_table_not_selected', value['distribution'])
        self.assertEqual(value['compiler_runtime']['static_library']['sha256'],
                         hashlib.sha256(self.gcc_archive.read_bytes()).hexdigest())
        self.assertFalse(value['load_dynamic'])
        self.assertEqual(len(value['dependency_fingerprints']), 6)

    def test_linux_link_output_cannot_select_copy_outside_pinned_cache(self):
        self.linux_fixture()
        wrong = self.root / 'external'
        wrong.mkdir()
        shutil.copyfile(self.library, wrong / self.library.name)
        self.output.write_text(f'cargo:rustc-link-search=native={wrong}\ncargo:rustc-link-lib=static=onnxruntime\n')
        with self.assertRaisesRegex(ValueError, 'selected Linux library differs'):
            self.collect()

    def test_linux_cached_archive_tamper_refused_after_completed_build(self):
        self.linux_fixture()
        self.library.write_bytes(b'!<arch>\ntamper\n')
        with self.assertRaisesRegex(ValueError, 'checksum or size differs'):
            self.collect()

    def test_linux_requires_explicit_repository_lock(self):
        self.target = 'x86_64-unknown-linux-gnu'
        with self.assertRaises(FileNotFoundError):
            self.collect()

    def test_linux_missing_or_ambiguous_compiler_runtime_link_refused(self):
        self.linux_fixture()
        original = self.gcc_output.read_text()
        for text in (f'cargo:rerun-if-changed={self.gcc_archive}\n',
                     original + 'cargo:rustc-link-arg=-lgcc\n'):
            self.gcc_output.write_text(text)
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, 'compiler runtime link argument'):
                self.collect()

    def test_linux_compiler_archive_change_must_be_tracked(self):
        self.linux_fixture()
        self.gcc_output.write_text(f'cargo:rustc-link-arg={self.gcc_archive}\n')
        with self.assertRaisesRegex(ValueError, 'invalidate changed build inputs'):
            self.collect()


if __name__ == '__main__':
    unittest.main()
