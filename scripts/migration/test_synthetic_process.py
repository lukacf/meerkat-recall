"""Real tiny Python subprocess controls; no fixture generation/product execution."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
import synthetic_process as p
import synthetic_scale as s


class ProcessControls(unittest.TestCase):
    def test_success_receipt_precedes_exec_and_complete_group_is_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            command=[sys.executable,'-c',
                "import json,pathlib; r=pathlib.Path('.'); x=json.loads(pathlib.Path(__import__('sys').argv[1]).read_text()); assert x['receipt_before_command_exec']; print('executed')",
                str(root/'ok.process.json')]
            result=s.phase('ok',command,root,os.environ.copy(),time.monotonic()+10,2**20,100)
            self.assertEqual(result['exit_code'],0)
            self.assertTrue(result['process_closure']['all_owned_processes_absent'])
            receipt=json.loads((root/'ok.process.json').read_text())
            self.assertEqual(receipt['leader']['pid'],receipt['leader']['pgid'])
            self.assertIsNone(p.identity(receipt['leader']['pid']))

    def test_timeout_kills_term_ignoring_child_in_separate_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            child="import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(30)"
            parent="import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',sys.argv[1]],start_new_session=True); time.sleep(30)"
            with (root/'log').open('wb') as log:
                phase=p.Phase([sys.executable,'-c',parent,child],root,'timeout',os.environ.copy(),log)
                until=time.monotonic()+3
                while len(phase.observe())<2 and time.monotonic()<until:time.sleep(.02)
                self.assertGreaterEqual(len(phase.observe()),2)
                time.sleep(.1)
                closure=phase.close(terminate=True,grace=.15)
            self.assertTrue(closure['all_owned_processes_absent'])
            self.assertTrue(all(p.identity(pid) is None for pid in phase.members))
            self.assertGreaterEqual(len(phase.members),2)

    def test_leader_exit_does_not_hide_background_descendant(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            command=[sys.executable,'-c',"import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'],start_new_session=True)"]
            with self.assertRaisesRegex(RuntimeError,'failed'):
                s.phase('orphan',command,root,os.environ.copy(),time.monotonic()+15,2**20,100)
            closure=json.loads((root/'orphan.closure.json').read_text())
            self.assertTrue(closure['all_owned_processes_absent'])
            self.assertTrue(closure['unexpected_live_descendants_at_exit'])

    def test_changed_starttime_never_signals_unrelated_process(self):
        child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])
        try:
            expected=p.identity(child.pid);expected['starttime']+=1
            self.assertFalse(p.signal_exact(expected,signal.SIGKILL))
            self.assertIsNone(child.poll())
        finally:
            child.kill();child.wait()

    def test_driver_sigterm_closes_separate_phase_group_before_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            script="""import sys,os,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
import synthetic_scale as s,synthetic_process as p
with p.interruptible():
 s.phase('signal',[sys.executable,'-c','import time;time.sleep(30)'],Path(sys.argv[2]),os.environ.copy(),time.monotonic()+30,2**20,100)
"""
            driver=subprocess.Popen([sys.executable,'-c',script,str(Path(__file__).parent),str(root)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            try:
                until=time.monotonic()+5
                while not (root/'signal.process.json').exists() and time.monotonic()<until:time.sleep(.02)
                self.assertTrue((root/'signal.process.json').exists())
                driver.send_signal(signal.SIGTERM)
                self.assertNotEqual(driver.wait(timeout=15),0)
                closure=json.loads((root/'signal.closure.json').read_text())
                self.assertTrue(closure['all_owned_processes_absent'])
                self.assertIsNone(p.identity(closure['leader']['pid']))
            finally:
                if driver.poll() is None:driver.kill();driver.wait()

    def test_signal_at_launch_receipt_boundary_cannot_escape_custody(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            original=p.durable_json
            def inject(path,value,replace=False):
                original(path,value,replace)
                if str(path).endswith('.process.json'):
                    os.kill(os.getpid(),signal.SIGTERM)
            with patch.object(p,'durable_json',side_effect=inject), p.interruptible():
                with self.assertRaises(InterruptedError):
                    s.phase('launch-signal',[sys.executable,'-c','import time;time.sleep(30)'],root,os.environ.copy(),time.monotonic()+30,2**20,100)
            closure=json.loads((root/'launch-signal.closure.json').read_text())
            self.assertTrue(closure['all_owned_processes_absent'])
            self.assertIsNone(p.identity(closure['leader']['pid']))

    def test_actual_deadline_stops_phase_and_preserves_failure_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with self.assertRaisesRegex(RuntimeError,'failed'):
                s.phase('deadline',[sys.executable,'-c','import time;time.sleep(30)'],root,os.environ.copy(),time.monotonic()+.05,2**20,100)
            result=json.loads((root/'deadline.result.json').read_text())
            self.assertEqual(result['resource_limit_failure'],'overall admission deadline')
            self.assertTrue(result['process_closure']['all_owned_processes_absent'])
            self.assertNotEqual(result['exit_code'],0)

    def test_pre_go_persistent_receipt_io_error_reaps_launcher(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); selected=[]
            error=OSError(28,'injected full device before GO')
            def fail(path,value,replace=False):
                if str(path).endswith('.process.json'):selected.append(value['leader'])
                raise error
            with patch.object(p,'durable_json',side_effect=fail):
                with self.assertRaises(OSError) as caught:
                    s.phase('io-before',[sys.executable,'-c',"from pathlib import Path;Path(__import__('sys').argv[1]).write_text('must not run')",str(root/'executed')],root,os.environ.copy(),time.monotonic()+10,2**20,100)
            self.assertIs(caught.exception,error)
            self.assertEqual(len(selected),1)
            self.assertIsNone(p.identity(selected[0]['pid']))
            self.assertFalse((root/'executed').exists())

    def test_post_go_membership_io_error_reaps_separate_session_descendant(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); selected=[]; durable=p.durable_json
            error=OSError(28,'injected full device after GO')
            def fail_after_children(path,value,replace=False):
                if str(path).endswith('.members.json') and len(value['members'])>=3:
                    selected.extend(value['members'])
                    raise error
                if selected:raise error
                return durable(path,value,replace)
            child="import time;time.sleep(30)"
            parent="import subprocess,sys,time;subprocess.Popen([sys.executable,'-c',sys.argv[1]],start_new_session=True);time.sleep(30)"
            with patch.object(p,'durable_json',side_effect=fail_after_children):
                with self.assertRaises(OSError) as caught:
                    s.phase('io-after',[sys.executable,'-c',parent,child],root,os.environ.copy(),time.monotonic()+10,2**20,100)
            self.assertIs(caught.exception,error)
            self.assertGreaterEqual(len(selected),3)
            self.assertTrue(all(p.identity(row['pid']) is None for row in selected))
            result=json.loads((root/'io-after.result.json').read_text())
            self.assertTrue(result['process_closure']['all_owned_processes_absent'])
            self.assertIn('receipt_error',result['process_closure'])

    def test_final_verification_over_budget_keeps_evidence_without_timing_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with patch.object(p.time,'monotonic',return_value=11):
                with self.assertRaisesRegex(RuntimeError,'final verification'):
                    p.finish_result(root,{'state':'semantic proof'},0,10,s.save)
            self.assertTrue((root/'verified-output.json').exists())
            self.assertFalse((root/'result.json').exists())

    def test_terminal_fsync_over_budget_refuses_successful_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with patch.object(p.time,'monotonic',side_effect=[9,11]):
                with self.assertRaisesRegex(RuntimeError,'terminal receipt'):
                    p.finish_result(root,{'state':'semantic proof'},0,10,s.save)
            self.assertTrue((root/'result.json').exists())
            # Presence alone never qualifies: caller records failure/nonzero.
            self.assertEqual(json.loads((root/'result.json').read_text())['seconds_including_generation'],9)

    def test_final_verification_in_budget_records_measured_elapsed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with patch.object(p.time,'monotonic',side_effect=[7,8]):
                p.finish_result(root,{'state':'semantic proof'},2,10,s.save)
            result=json.loads((root/'result.json').read_text())
            self.assertEqual(result['seconds_including_generation'],5)
            self.assertTrue(result['verification_and_evidence_fsync_within_deadline'])


if __name__=='__main__':unittest.main()
