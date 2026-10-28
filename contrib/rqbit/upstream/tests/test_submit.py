"""Submission safety regressions using real local Git and fake gh transport."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

KIT = Path(__file__).resolve().parents[1]
REPO = KIT.parents[2]


class SubmitTests(unittest.TestCase):
    def setUp(self):
        scratch = REPO / 'target' / 'upstream-submit-tests'
        scratch.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(prefix='review ', dir=scratch)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.git = shutil.which('git')
        self.env = os.environ.copy()
        self.env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1',
                        GIT_AUTHOR_NAME='Paul Junod', GIT_AUTHOR_EMAIL='donujp@gmail.com',
                        GIT_COMMITTER_NAME='Paul Junod', GIT_COMMITTER_EMAIL='donujp@gmail.com')
        self.up = self.root / 'upstream'
        self.kit = self.root / 'kit'
        self.kit.mkdir()
        self.run_git('init', '-q', '-b', 'main', str(self.up))
        (self.up / 'source').write_text('baseline\n')
        self.commit('Initial source')
        self.base = self.run_git('-C', str(self.up), 'rev-parse', 'HEAD').strip()
        (self.up / 'change').write_text('fix\n')
        self.commit('fix: correct source')
        patch = self.run_git('-C', str(self.up), 'format-patch', '-1', '--stdout')
        (self.kit / '01-metadata-size-limit.patch').write_text(patch)
        (self.kit / '01-metadata-size-limit.md').write_text('# Fix\n\n## AI disclosure\nAI-assisted; human review remains required.\n')
        self.run_git('-C', str(self.up), 'reset', '--hard', self.base)
        self.script = self.kit / 'submit.sh'
        shutil.copy(KIT / 'submit.sh', self.script)
        self.script.chmod(0o755)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.log = self.root / 'actions.jsonl'
        (self.bin / 'git').write_text('''#!/usr/bin/env python3
import json,os,subprocess,sys
args=sys.argv[1:]
if 'fetch' in args and os.environ.get('FAIL_FETCH'): sys.exit(42)
if 'ls-remote' in args:
 if os.environ.get('REMOTE_TIP'): print(os.environ['REMOTE_TIP']+'\\trefs/heads/up/metadata-size-limit')
 sys.exit(0)
if 'push' in args:
 with open(os.environ['ACTION_LOG'],'a') as f: f.write(json.dumps({'git':args})+'\\n')
 sys.exit(0)
sys.exit(subprocess.call([os.environ['REAL_GIT'],*args]))
''')
        (self.bin / 'git').chmod(0o755)
        (self.bin / 'gh').write_text('''#!/usr/bin/env python3
import json,os,sys
args=sys.argv[1:]
with open(os.environ['ACTION_LOG'],'a') as f: f.write(json.dumps({'gh':args})+'\\n')
if args[:2]==['api','user']: print('pjunod')
elif args[:2]==['pr','list']:
 if os.environ.get('FAIL_PR_LOOKUP'): sys.exit(42)
 print('')
elif args[:2]==['repo','view']:
 if '--json' in args: print(os.environ.get('FORK_PARENT','ikatson/rqbit'))
 elif os.environ.get('FORK_INITIAL_LOOKUP_FAIL'):
  records=[json.loads(x) for x in open(os.environ['ACTION_LOG']).read().splitlines()]
  if sum(x.get('gh',[])[:2]==['repo','view'] for x in records)==1: sys.exit(42)
elif args[:2]==['repo','fork']:
 if os.environ.get('FAIL_FORK'): sys.exit(42)
elif args[:2]==['pr','create']: print('https://github.com/ikatson/rqbit/pull/123')
elif args[:2]==['issue','list']: print(json.dumps([{'title':os.environ.get('ISSUE_TITLE',''),'url':'https://github.com/ikatson/rqbit/issues/123'}]))
else: sys.exit(80)
''')
        (self.bin / 'gh').chmod(0o755)
        self.work = self.root / 'cache'
        self.env.update(PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
                        REAL_GIT=self.git, ACTION_LOG=str(self.log),
                        RQBIT_UPSTREAM_URL=str(self.up), RQBIT_SUBMIT_WORKDIR=str(self.work))

    def run_git(self, *args):
        return subprocess.check_output([self.git, *args], env=self.env, text=True, stderr=subprocess.DEVNULL)

    def commit(self, title):
        self.run_git('-C', str(self.up), 'add', '.')
        self.run_git('-C', str(self.up), 'commit', '-q', '-m', title)

    def invoke(self, *args, **extra):
        return subprocess.run([str(self.script), *args], env={**self.env, **extra}, capture_output=True, text=True)

    def actions(self):
        return [json.loads(x) for x in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_issue_previews_make_no_gh_calls(self):
        for unit in ('10', '11', '13'):
            r = subprocess.run([str(KIT / 'submit-issue.sh'), unit, '--dry-run'],
                               env=self.env, capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn('Would open on ikatson/rqbit:', r.stdout)
        self.assertEqual(self.actions(), [])

    def test_unsigned_dry_run_uses_current_source_without_gh(self):
        r = self.invoke('metadata-size-limit', '--dry-run')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.actions(), [])
        message = self.run_git('-C', str(self.work / 'rqbit'), 'log', '-1', '--format=%B')
        self.assertNotIn('Signed-off-by:', message)
        self.assertEqual((self.work / 'rqbit/change').read_text(), 'fix\n')

    def test_fetch_failure_under_all_stops_before_patch_or_push(self):
        r = self.invoke('all', '--dry-run', FAIL_FETCH='1')
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('refusing stale source', r.stderr)
        self.assertEqual(self.actions(), [])
        self.assertFalse((self.work / 'rqbit/change').exists())

    def test_unowned_cache_preserves_source(self):
        clone = self.work / 'rqbit'
        clone.mkdir(parents=True)
        (clone / 'keep').write_text('user source')
        r = self.invoke('metadata-size-limit', '--dry-run')
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('refusing unowned cache', r.stderr)
        self.assertEqual((clone / 'keep').read_text(), 'user source')

    def test_dirty_owned_cache_preserves_untracked_files(self):
        first = self.invoke('metadata-size-limit', '--dry-run')
        self.assertEqual(first.returncode, 0, first.stderr)
        keep = self.work / 'rqbit/keep'
        keep.write_text('user source')
        r = self.invoke('metadata-size-limit', '--dry-run')
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('cache has local changes', r.stderr)
        self.assertEqual(keep.read_text(), 'user source')

    def qualify_fixture(self):
        self.script.write_text(self.script.read_text().replace(
            'QUALIFIED_BASE=f4ca7d919638a7b8b031ab062a61861a9eb586e4',
            'QUALIFIED_BASE=' + self.base))

    def test_real_path_uses_explicit_lease_with_mocked_git_and_gh(self):
        self.qualify_fixture()
        r = self.invoke('metadata-size-limit')
        self.assertEqual(r.returncode, 0, r.stderr)
        pushes = [x['git'] for x in self.actions() if 'git' in x and 'push' in x['git']]
        self.assertEqual(len(pushes), 1)
        self.assertTrue(any(a.startswith('--force-with-lease=') for a in pushes[0]))
        self.assertTrue(any(x.get('gh', [])[:2] == ['pr','create'] for x in self.actions()))
        message = self.run_git('-C', str(self.work / 'rqbit'), 'log', '-1', '--format=%B')
        self.assertEqual(message.count('Signed-off-by: Paul Junod <donujp@gmail.com>'), 1)
        lookups = [x['gh'] for x in self.actions() if x.get('gh',[])[:2] == ['pr','list']]
        self.assertTrue(all('--base' in args and args[args.index('--base')+1] == 'main' for args in lookups))

    def test_fork_creation_failure_stops_before_push(self):
        self.qualify_fixture()
        r = self.invoke('metadata-size-limit', FORK_INITIAL_LOOKUP_FAIL='1', FAIL_FORK='1')
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('fork creation failed', r.stderr)
        self.assertFalse(any('git' in x for x in self.actions()))
        self.assertFalse(any(x.get('gh',[])[:2] == ['pr','create'] for x in self.actions()))

    def test_newly_appeared_foreign_repository_stops_before_push(self):
        self.qualify_fixture()
        r = self.invoke('metadata-size-limit', FORK_INITIAL_LOOKUP_FAIL='1', FORK_PARENT='foreign/project')
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('is not a fork of ikatson/rqbit', r.stderr)
        self.assertTrue(any(x.get('gh',[])[:2] == ['repo','fork'] for x in self.actions()))
        self.assertFalse(any('git' in x for x in self.actions()))

    def test_existing_foreign_repository_stops_before_push(self):
        self.qualify_fixture()
        r = self.invoke('metadata-size-limit', FORK_PARENT='foreign/project')
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('is not a fork of ikatson/rqbit', r.stderr)
        self.assertFalse(any('git' in x for x in self.actions()))

    def test_real_path_preserves_unowned_remote_tip(self):
        self.qualify_fixture()
        r = self.invoke('metadata-size-limit', REMOTE_TIP=self.base)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('refusing unowned or changed remote branch', r.stderr)
        self.assertFalse(any('git' in x for x in self.actions()))

    def test_failed_pr_lookup_stops_before_push(self):
        self.qualify_fixture()
        r = self.invoke('metadata-size-limit', FAIL_PR_LOOKUP='1')
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('cannot inspect existing PRs', r.stderr)
        self.assertFalse(any('git' in x for x in self.actions()))

    def test_issue_rerun_returns_existing_url_without_posting(self):
        title = (KIT / 'issues/11-disable-pex.md').read_text().splitlines()[0][2:]
        r = subprocess.run([str(KIT / 'submit-issue.sh'), '11'],
            env={**self.env, 'ISSUE_TITLE':title}, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), 'https://github.com/ikatson/rqbit/issues/123')
        self.assertFalse(any(x.get('gh', [])[:2] == ['issue','create'] for x in self.actions()))

    def test_real_submission_refuses_unqualified_base_before_mutation(self):
        r = self.invoke('metadata-size-limit')
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('upstream moved from qualified base', r.stderr)
        self.assertFalse(any('git' in x and 'push' in x['git'] for x in self.actions()))
        self.assertFalse(any(x.get('gh', [])[:2] == ['pr', 'create'] for x in self.actions()))


if __name__ == '__main__':
    unittest.main()
