"""Small packaging contract checks; no Docker, servers, Rust or provider calls."""
import importlib.machinery
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import tomllib
import unittest
from unittest import mock

import container_delivery as delivery
import host_port_smoke
import promote_containers as promotion

HERE = Path(__file__).resolve().parent
loader = importlib.machinery.SourceFileLoader('prepare_compose', str(HERE/'prepare-compose'))
spec = importlib.util.spec_from_loader(loader.name, loader)
compose = importlib.util.module_from_spec(spec); loader.exec_module(compose)


class ComposeTests(unittest.TestCase):
    def images(self):
        return ('ghcr.io/lukacf/meerkat-recall@sha256:'+'a'*64,
                'ghcr.io/lukacf/recall-surreal@sha256:'+'b'*64)

    def test_fresh_private_distinct_keys_and_complete_toml(self):
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)/'deployment'
            result=compose.prepare(output,*self.images())
            self.assertFalse(result['started'])
            keys=[(output/'secrets'/name).read_text().strip() for name in ('signing-key','database-password')]
            self.assertNotEqual(*keys)
            self.assertEqual(output.stat().st_mode&0o777,0o700)
            for p in (output/'secrets').iterdir():
                self.assertEqual(p.stat().st_mode&0o777,0o600)
            cfg=tomllib.loads((output/'secrets/runtime.toml').read_text())
            self.assertEqual(cfg['storage']['surreal_db']['password'],keys[1])
            self.assertEqual(cfg['extractor']['mcp_url'],'http://127.0.0.1:3000/mcp/agent')
            self.assertTrue(cfg['extractor']['required'])
            self.assertNotIn(keys[0],json.dumps(result))
            with self.assertRaises(FileExistsError):compose.prepare(output,*self.images())
            self.assertEqual((output/'secrets/signing-key').read_text().strip(),keys[0])

    def test_wrong_role_mutable_image_and_symlink_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)/'deployment'
            with self.assertRaises(ValueError):compose.prepare(output,self.images()[1],self.images()[0])
            with self.assertRaises(ValueError):compose.prepare(output,'ghcr.io/lukacf/meerkat-recall:latest',self.images()[1])
            actual=Path(temp)/'actual';actual.mkdir();output.symlink_to(actual)
            with self.assertRaises(ValueError):compose.prepare(output,*self.images())

    def test_secret_adapter_refuses_unsafe_modes_and_conflicting_values(self):
        script=HERE.parents[1]/'packaging/recall-container/entrypoint.sh'
        with tempfile.TemporaryDirectory() as temp:
            secret=Path(temp)/'key';secret.write_text('fixture');secret.chmod(0o644)
            env={'PATH':'/usr/bin:/bin','SURREALDB_PASS_FILE':str(secret)}
            r=subprocess.run(['sh',str(script),'true'],env=env,capture_output=True)
            self.assertNotEqual(r.returncode,0)
            secret.chmod(0o600);env['SURREALDB_PASS']='conflicting'
            self.assertNotEqual(subprocess.run(['sh',str(script),'true'],env=env,capture_output=True).returncode,0)
            del env['SURREALDB_PASS']
            self.assertEqual(subprocess.run(['sh',str(script),'sh','-c','test "$SURREALDB_PASS" = fixture'],env=env,capture_output=True).returncode,0)


class DeliveryTests(unittest.TestCase):
    def test_registry_absence_errors_and_immutable_tag_conflicts(self):
        import hashlib
        expected = 'sha256:' + 'a'*64
        raw = json.dumps({'schemaVersion': 2, 'config': {'digest': expected}}).encode()
        digest = 'sha256:' + hashlib.sha256(raw).hexdigest()
        self.assertEqual(promotion.classify_manifest(200, {'Docker-Content-Digest': digest}, raw, expected), digest)
        absent = b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}'
        self.assertIsNone(promotion.classify_manifest(404, {}, absent, expected))
        for status, body in ((401, absent), (403, absent), (500, absent), (404, b'{"errors":[{"code":"DENIED"}]}')):
            with self.assertRaises(ValueError): promotion.classify_manifest(status, {}, body, expected)
        with self.assertRaises(ValueError): promotion.classify_manifest(200, {'Docker-Content-Digest': digest}, raw, 'sha256:'+'b'*64)
        with self.assertRaises(ValueError): promotion.classify_manifest(200, {'Docker-Content-Digest': 'sha256:'+'b'*64}, raw, expected)

    def test_release_tag_source_and_normal_draft_are_required(self):
        release = {'draft': True, 'prerelease': False, 'tag_name': 'v0.9.1'}
        with mock.patch.object(promotion.subprocess, 'check_output', return_value=json.dumps({'object': {'type': 'commit', 'sha': 'a'*40}}).encode()):
            promotion.verify_release_binding(release, 'v0.9.1', 'a'*40)
            with self.assertRaises(ValueError): promotion.verify_release_binding(release, 'v0.9.1', 'b'*40)
            with self.assertRaises(ValueError): promotion.verify_release_binding({**release, 'draft': False}, 'v0.9.1', 'a'*40)

    def test_draft_lookup_uses_numeric_id_and_rejects_invalid_identity(self):
        release = {'id': 413257021, 'draft': True, 'prerelease': False, 'tag_name': 'v0.9.1'}
        def output(argv, **_):
            # The real REST tag lookup is404 for drafts; never use that route.
            if '/releases/tags/' in ' '.join(argv):
                raise subprocess.CalledProcessError(1, argv)
            if argv[:3] == ['gh', 'release', 'view']:
                return json.dumps({'databaseId': 413257021}).encode()
            self.assertEqual(argv, ['gh', 'api', 'repos/lukacf/meerkat-recall/releases/413257021'])
            return json.dumps(release).encode()
        with mock.patch.object(promotion.subprocess, 'check_output', side_effect=output):
            self.assertEqual(promotion.draft_release('v0.9.1'), release)
        for invalid in (None, 0, -1, True, '413257021'):
            with mock.patch.object(promotion.subprocess, 'check_output', return_value=json.dumps({'databaseId': invalid}).encode()) as called:
                with self.assertRaises(ValueError): promotion.draft_release('v0.9.1')
                self.assertEqual(called.call_count, 1)
        with mock.patch.object(promotion.subprocess, 'check_output', side_effect=[b'{"databaseId":413257021}', b'{"id":413257022}']):
            with self.assertRaises(ValueError): promotion.draft_release('v0.9.1')

    def test_newer_main_tooling_keeps_exact_qualified_checkout(self):
        env = {'GITHUB_ACTIONS': 'true', 'GITHUB_REPOSITORY': 'lukacf/meerkat-recall',
               'GITHUB_REF': 'refs/heads/main', 'GITHUB_SHA': 'b'*40}
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(promotion.subprocess, 'check_output', side_effect=['b'*40+'\n', 'a'*40+'\n']):
                self.assertEqual(promotion.verify_dispatch_source('a'*40, '/qualified-source'), 'b'*40)
            for heads in (['a'*40+'\n'], ['b'*40+'\n', 'c'*40+'\n']):
                with mock.patch.object(promotion.subprocess, 'check_output', side_effect=heads):
                    with self.assertRaises(ValueError): promotion.verify_dispatch_source('a'*40, '/qualified-source')
            with self.assertRaises(ValueError): promotion.verify_dispatch_source('main', '/qualified-source')
        for key, bad in [('GITHUB_ACTIONS', 'false'), ('GITHUB_REPOSITORY', 'other/repo'), ('GITHUB_REF', 'refs/tags/v0.9.1')]:
            with mock.patch.dict(os.environ, {**env, key: bad}, clear=True):
                with mock.patch.object(promotion.subprocess, 'check_output') as called:
                    with self.assertRaises(ValueError): promotion.verify_dispatch_source('a'*40, '/qualified-source')
                    called.assert_not_called()

    def test_bad_source_or_release_binding_fails_before_registry_or_docker(self):
        release = {'id': 413257021, 'draft': True, 'prerelease': False, 'tag_name': 'v0.9.1'}
        value = {'version': '0.9.1', 'source_commit': 'a'*40, 'images': {}}
        with tempfile.TemporaryDirectory() as temp:
            for changes, manifest_source, tag_source in [({'draft': False}, 'a'*40, 'a'*40),
                    ({'prerelease': True}, 'a'*40, 'a'*40), ({'tag_name': 'v0.9.2'}, 'a'*40, 'a'*40),
                    ({}, 'a'*40, 'c'*40), ({}, 'c'*40, 'a'*40)]:
                with mock.patch.object(promotion, 'verify_dispatch_source', return_value='b'*40), \
                     mock.patch.object(delivery, 'verify', return_value={**value, 'source_commit': manifest_source}), \
                     mock.patch.object(promotion, 'draft_release', return_value={**release, **changes}), \
                     mock.patch.object(promotion.subprocess, 'check_output', return_value=json.dumps({'object': {'type': 'commit', 'sha': tag_source}}).encode()), \
                     mock.patch.object(promotion, 'registry_manifest') as registry, \
                     mock.patch.object(promotion.subprocess, 'run') as mutation:
                    with self.assertRaises(ValueError): promotion.promote(temp, 'd'*64, 'v0.9.1', 'a'*40, '/qualified-source')
                    registry.assert_not_called()
                    mutation.assert_not_called()
                    self.assertFalse((Path(temp)/'publication-binding.json').exists())

    def test_explicit_source_success_preserves_config_guard_and_tooling_record(self):
        release = {'id': 413257021, 'draft': True, 'prerelease': False, 'tag_name': 'v0.9.1'}
        image = {'repository': 'ghcr.io/lukacf/meerkat-recall', 'image_id': 'sha256:'+'c'*64, 'archive': 'meerkat-recall.docker.tar'}
        value = {'version': '0.9.1', 'source_commit': 'a'*40, 'images': {'recall': image}}
        for loaded_id in (image['image_id'], 'sha256:'+'f'*64):
            with tempfile.TemporaryDirectory() as temp:
                responses = [json.dumps({'object': {'type': 'commit', 'sha': 'a'*40}}).encode(), json.dumps([{'Id': loaded_id}]).encode()]
                with mock.patch.object(promotion, 'verify_dispatch_source', return_value='b'*40), \
                     mock.patch.object(delivery, 'verify', return_value=value), \
                     mock.patch.object(promotion, 'draft_release', return_value=release), \
                     mock.patch.object(promotion.subprocess, 'check_output', side_effect=responses), \
                     mock.patch.object(promotion, 'registry_manifest', side_effect=[None, None, 'sha256:'+'e'*64]), \
                     mock.patch.object(promotion.subprocess, 'run') as mutation:
                    if loaded_id == image['image_id']:
                        promotion.promote(temp, 'd'*64, 'v0.9.1', 'a'*40, '/qualified-source')
                        self.assertEqual([call.args[0][1] for call in mutation.call_args_list], ['load', 'tag', 'push'])
                        binding = json.loads((Path(temp)/'publication-binding.json').read_text())
                        self.assertEqual(binding['tooling_commit'], 'b'*40)
                        self.assertEqual(binding['qualified_source_commit'], 'a'*40)
                        self.assertEqual(binding['release_id'], 413257021)
                    else:
                        with self.assertRaises(ValueError): promotion.promote(temp, 'd'*64, 'v0.9.1', 'a'*40, '/qualified-source')
                        self.assertEqual([call.args[0][1] for call in mutation.call_args_list], ['load'])

    def test_wrong_source_license_and_paired_capability_refused(self):
        labels={'org.opencontainers.image.source':delivery.REPOSITORY,
                'org.opencontainers.image.revision':'a'*40,'org.opencontainers.image.licenses':'BUSL-1.1',
                'org.meerkat.recall.reference-token-version':'1',
                'org.meerkat.recall.paired-upstream':'8afd2ba0b6c0e232f19f42070e8811b323324c4c'}
        image={'Os':'linux','Architecture':'amd64','Config':{'Labels':labels}}
        delivery.validate_image(image,'a'*40,'paired')
        for key,value in [('org.opencontainers.image.revision','b'*40),('org.opencontainers.image.licenses','MIT'),('org.meerkat.recall.reference-token-version','0')]:
            altered=json.loads(json.dumps(image));altered['Config']['Labels'][key]=value
            with self.assertRaises(ValueError):delivery.validate_image(altered,'a'*40,'paired')

    def test_export_no_qualification_does_not_contact_docker(self):
        with tempfile.TemporaryDirectory() as temp:
            selected=Path(temp)/'selection';selected.write_text(json.dumps({'approved':True,'reviewer':'release-owner','host_port_smoke_passed':False}))
            with mock.patch.object(delivery.subprocess,'check_output') as called:
                with self.assertRaises(ValueError):delivery.export_images(selected,Path(temp)/'out')
                called.assert_not_called()

    def test_fake_docker_digest_and_unsafe_archive_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'image.tar'
            for name in ('../escape','manifest.json'):
                with tarfile.open(path,'w') as tar:
                    raw=json.dumps([{'Config':'missing.json'}]).encode();info=tarfile.TarInfo(name);info.size=len(raw);tar.addfile(info,io.BytesIO(raw))
                item={'archive_bytes':path.stat().st_size,'archive_sha256':delivery.digest(path),'image_id':'sha256:'+'a'*64}
                with self.assertRaises((ValueError,KeyError)):delivery.verify_archive(path,item,'b'*40,'recall')


class HostPortTests(unittest.TestCase):
    def test_null_wildcard_host_and_foreign_container_are_not_host_port_success(self):
        cid='a'*64
        good={'Id':cid,'State':{'Running':True},'HostConfig':{'NetworkMode':'bridge'},'NetworkSettings':{'Ports':{'3000/tcp':[{'HostIp':'127.0.0.1','HostPort':'3000'}]}}}
        host_port_smoke.validate_mapping(good,cid,3000)
        for mapping in (None,[],[{'HostIp':'0.0.0.0','HostPort':'3000'}]):
            bad=json.loads(json.dumps(good));bad['NetworkSettings']['Ports']['3000/tcp']=mapping
            with self.assertRaises(ValueError):host_port_smoke.validate_mapping(bad,cid,3000)
        with self.assertRaises(ValueError):host_port_smoke.validate_mapping(good,'b'*64,3000)


if __name__=='__main__':unittest.main()
