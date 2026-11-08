import { holdCauseLabel, holdDeadlineSeconds, holdDeadlineText, holdHeadline } from '../src/holdPresentation';
import { JobControl } from '../src/api/types';

const control = (patch: Partial<JobControl>): JobControl => ({
  version: 1, revision: '1', lifecycle: 'held', cause: 'capacity', stage: 'download_write',
  retry_policy: 'resume_same_job', message: 'storage full', instance: 'i', ...patch,
});

test('a hold names its cause, then Runner\'s message', () => {
  expect(holdHeadline(control({}))).toBe('out of space: storage full');
  expect(holdHeadline(control({ cause: 'password_required', message: 'needs a password' }))).toBe('password required: needs a password');
});
test('a cause from a newer node is shown as it came', () => {
  expect(holdCauseLabel('brand_new_cause')).toBe('brand_new_cause');
});
test('deadline_unix counts down live against the device clock', () => {
  const c = control({ deadline_unix: 10_000, deadline_in_s: 99999 });
  expect(holdDeadlineSeconds(c, 7_000_000)).toBe(3000);
  expect(holdDeadlineText(c, 7_000_000)).toBe('Fails as hold expired in 50m unless it recovers');
  expect(holdDeadlineSeconds(c, 20_000_000)).toBe(0);
});
test('deadline_in_s is used when no unix time came', () => {
  expect(holdDeadlineText(control({ deadline_in_s: 7200 }), 0)).toBe('Fails as hold expired in 2h unless it recovers');
});
test('a hold without a deadline says nothing about one', () => {
  expect(holdDeadlineText(control({}), 0)).toBeNull();
});
