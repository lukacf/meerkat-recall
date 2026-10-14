"""Offline asset identity tests; fixture bytes are never used for inference."""
import hashlib
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

loader = importlib.machinery.SourceFileLoader('provision_fastembed', str(Path(__file__).with_name('provision-fastembed')))
spec = importlib.util.spec_from_loader(loader.name, loader)
provision = importlib.util.module_from_spec(spec)
loader.exec_module(provision)


class FastEmbedAssetsTests(unittest.TestCase):
    def test_bge_recipe_versions_manifest_without_changing_model_bytes_or_old_bundle(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'source'
            source.mkdir()
            originals = {name: f'fixture {name}'.encode()
                         for name in ['model.onnx', *provision.TOKENIZER_FILES.values()]}
            for name, raw in originals.items():
                (source / name).write_bytes(raw)
            before = provision.install_assets(root / 'assets', 'bge-small-en-v1.5', source / 'model.onnx', source, {})
            before_directory = Path(before['asset_directory'])
            before_manifest = (before_directory / 'manifest.json').read_bytes()
            after = provision.install_assets(root / 'assets', 'bge-small-en-v1.5', source / 'model.onnx', source, {},
                                             preprocessing='bge-en-retrieval-v1')
            self.assertNotEqual(before['model_version'], after['model_version'])
            self.assertEqual(after['preprocessing'], 'bge-en-retrieval-v1')
            after_manifest = json.loads((Path(after['asset_directory']) / 'manifest.json').read_bytes())
            self.assertEqual(after_manifest['preprocessing'], 'bge-en-retrieval-v1')
            after_manifest['preprocessing'] = 'text-v1'
            self.assertEqual(after_manifest, json.loads(before_manifest))
            self.assertEqual((before_directory / 'manifest.json').read_bytes(), before_manifest)
            for name, raw in originals.items():
                self.assertEqual((source / name).read_bytes(), raw)
                self.assertEqual((before_directory / name).read_bytes(), raw)
                self.assertEqual((Path(after['asset_directory']) / name).read_bytes(), raw)

    def test_bge_recipe_refuses_unsupported_model_or_version_before_creating_asset_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / 'not-created'
            for model, preprocessing in [('all-minilm-l6-v2', 'bge-en-retrieval-v1'),
                                         ('bge-small-en-v1.5', 'bge-en-retrieval-v2')]:
                with self.assertRaises(ValueError):
                    provision.install_assets(root, model, Path('/missing/model'), Path('/missing/tokenizer'), {},
                                             preprocessing=preprocessing)
                self.assertFalse(root.exists())

    def test_manifest_version_binds_every_copied_byte_and_retains_exact_model(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'source'
            source.mkdir()
            for name in ['model.onnx', *provision.TOKENIZER_FILES.values()]:
                (source / name).write_bytes(f'fixture {name}'.encode())
            result = provision.install_assets(root / 'assets', 'bge-small-en-v1.5', source / 'model.onnx', source, {})
            directory = Path(result['asset_directory'])
            encoded = (directory / 'manifest.json').read_bytes()
            self.assertEqual(result['model_version'], 'sha256:' + hashlib.sha256(encoded).hexdigest())
            manifest = json.loads(encoded)
            self.assertEqual(manifest['model'], 'bge-small-en-v1.5')
            self.assertEqual(manifest['dimensions'], 384)
            for key in ['onnx', *provision.TOKENIZER_FILES]:
                asset = manifest[key]
                copied = (directory / asset['path']).read_bytes()
                self.assertEqual(asset['sha256'], hashlib.sha256(copied).hexdigest())
                self.assertEqual(asset['bytes'], len(copied))
            with self.assertRaisesRegex(ValueError, 'already exists'):
                provision.install_assets(root / 'assets', 'bge-small-en-v1.5', source / 'model.onnx', source, {})
            (source / 'model.onnx').write_bytes(b'changed model bytes')
            changed = provision.install_assets(root / 'assets', 'bge-small-en-v1.5', source / 'model.onnx', source, {})
            self.assertNotEqual(changed['model_version'], result['model_version'])

    def test_missing_assets_and_invalid_external_names_never_publish_version(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'source'
            source.mkdir()
            for name in ['model.onnx', *provision.TOKENIZER_FILES.values()]:
                (source / name).write_bytes(b'fixture')
            with self.assertRaisesRegex(ValueError, 'relative ONNX'):
                provision.install_assets(root / 'assets', 'bge-small-en-v1.5', source / 'model.onnx', source, {'../escape': source / 'model.onnx'})
            (source / 'config.json').unlink()
            with self.assertRaisesRegex(ValueError, 'bounded regular'):
                provision.install_assets(root / 'assets', 'bge-small-en-v1.5', source / 'model.onnx', source, {})
            self.assertEqual(list((root / 'assets').iterdir()), [])


if __name__ == '__main__':
    unittest.main()
