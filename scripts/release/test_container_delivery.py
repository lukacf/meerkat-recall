"""Small packaging contract checks; no Docker, servers, Rust or provider calls."""
import importlib.machinery
import importlib.util
import io
import json
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
