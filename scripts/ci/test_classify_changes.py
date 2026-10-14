#!/usr/bin/env python3
"""Boundary tests for prose classification, including real Git merge results."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('classifier', Path(__file__).with_name('classify_changes.py'))
classifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(classifier)


class ProseTests(unittest.TestCase):
    def test_reader_prose(self):
        self.assertTrue(classifier.prose_only('README.md', b'# Recall\nOld words.\n', b'# Recall\nClear words.\n'))
        self.assertTrue(classifier.prose_only('docs/operations/development.mdx', b'---\ntitle: Dev\n---\nOld words\n', b'---\ntitle: Dev\n---\nNew words\n'))

    def test_nonprose_paths(self):
        for path in ['.github/workflows/ci.yml', 'scripts/README.md', 'docs/docs.json', 'Cargo.toml', 'Cargo.lock', '.config/nextest.toml', 'docs/examples/setup.md', 'docs/internal/plan.md', 'examples/README.md', 'docs/architecture/living-wiki.mdx', 'unknown.md']:
            with self.subTest(path=path):
                self.assertFalse(classifier.prose_only(path, b'old', b'new'))

    def test_code_and_configuration_changes(self):
        for old,new in [(b'```sh\nrun old\n```\n', b'```sh\nrun new\n```\n'), (b'~~~json\n{"port":1}\n~~~\n', b'~~~json\n{"port":2}\n~~~\n'), (b'Use `old`.\n', b'Use `new`.\n'), (b'    old\n', b'    new\n'), (b'<Card href="/old">\n', b'<Card href="/new">\n'), (b'---\ntitle: Old\n---\n', b'---\ntitle: New\n---\n'), (b'Old\n', b'```unclosed\n'), (b'old\x00', b'new\x00'), (b'Old\n',b'\xff')]:
            with self.subTest(old=old):
                self.assertFalse(classifier.prose_only('README.md', old, new))

    def test_multiline_mdx_and_nested_code_are_not_prose(self):
        examples = [
            '<Card\n  href="old"\n>\nText\n</Card>\n',
            '{\n  old\n}\n',
            '> ```sh\n> old\n> ```\n',
            '- ~~~sh\n  old\n  ~~~\n',
        ]
        for old in examples:
            with self.subTest(example=old):
                self.assertFalse(classifier.prose_only('docs/installation.mdx',old.encode(),old.replace('old','new').encode()))

    def test_unchanged_examples_remain_eligible(self):
        old=b'Old words\n```sh\nmake check\n```\n'
        self.assertTrue(classifier.prose_only('README.md',old,old.replace(b'Old',b'Clear')))


class GitTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.git('init','-q','-b','main');self.git('config','user.name','Fixture');self.git('config','user.email','fixture@example.invalid')
        (self.root/'README.md').write_text('Initial prose.\n');self.base=self.commit()

    def git(self,*args):
        return subprocess.check_output(['git','-C',str(self.root),*args],stderr=subprocess.PIPE,text=True).strip()

    def commit(self):
        self.git('add','-A');self.git('commit','-qm','fixture');return self.git('rev-parse','HEAD')

    def changed(self):
        (self.root/'README.md').write_text('Revised prose.\n');return self.commit()

    def test_modified_prose_and_manual_full(self):
        head=self.changed();self.assertTrue(classifier.classify(self.root,'push',self.base,head)['docs_only'])
        self.assertFalse(classifier.classify(self.root,'workflow_dispatch','',head)['docs_only'])
        self.assertFalse(classifier.classify(self.root,'push','0'*40,head)['docs_only'])
        self.assertFalse(classifier.classify(self.root,'unknown',self.base,head)['docs_only'])
        self.assertFalse(classifier.classify(self.root,'push',head,head)['docs_only'])

    def test_added_deleted_renamed_or_executable_file_full(self):
        for operation in ['added','deleted','renamed','mode','symlink']:
            with self.subTest(operation=operation):
                self.git('reset','--hard',self.base);self.git('clean','-fd')
                p=self.root/'README.md'
                if operation=='added': (self.root/'CHANGELOG.md').write_text('New prose\n')
                elif operation=='deleted': p.unlink()
                elif operation=='renamed': p.rename(self.root/'CHANGELOG.md')
                elif operation=='mode': p.chmod(0o755)
                else: p.unlink();p.symlink_to('other')
                head=self.commit();self.assertFalse(classifier.classify(self.root,'push',self.base,head)['docs_only'])

    def test_missing_or_untrusted_base_fails(self):
        head=self.changed()
        for base in ['', 'refs/heads/main', 'f'*40]:
            with self.subTest(base=base),self.assertRaises((ValueError,subprocess.CalledProcessError)):
                classifier.classify(self.root,'pull_request',base,head)
        with self.assertRaises(ValueError): classifier.classify(self.root,'pull_request',self.base,head)

    def test_pr_merge_uses_current_base_not_stale_branch_point(self):
        self.git('checkout','-qb','topic');self.changed();self.git('checkout','main')
        (self.root/'runtime.rs').write_text('fn changed_on_base() {}\n');base=self.commit()
        self.git('merge','--no-ff','-qm','merge','topic');head=self.git('rev-parse','HEAD')
        self.assertTrue(classifier.classify(self.root,'pull_request',base,head)['docs_only'])
        with self.assertRaises(ValueError): classifier.classify(self.root,'pull_request',self.base,head)

    def test_mixed_or_newline_path_is_not_hidden(self):
        self.changed();(self.root/'runtime\nREADME.md').write_text('code');head=self.commit()
        self.assertFalse(classifier.classify(self.root,'push',self.base,head)['docs_only'])


if __name__=='__main__': unittest.main()
