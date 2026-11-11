import { formatAppVersion } from '../src/app-version-format';

describe('app version formatting', () => {
  it('includes the native build number when it is available', () => {
    expect(formatAppVersion('1.1.0', '3')).toBe('1.1.0 (3)');
    expect(formatAppVersion('1.1.0', 3)).toBe('1.1.0 (3)');
  });

  it('handles development and incomplete manifests', () => {
    expect(formatAppVersion('1.1.0', null)).toBe('1.1.0');
    expect(formatAppVersion(undefined, undefined)).toBe('Unknown');
  });
});

// Device builds come from the committed native projects, not app.json, so a
// release that bumps only app.json ships under the old version and build
// number. 1.1.5 through 1.1.7 did exactly that and installed as 1.1.4.
describe('native projects carry the app.json release', () => {
  const fs = require('fs') as typeof import('fs');
  const path = require('path') as typeof import('path');
  const root = path.join(__dirname, '..');
  const expo = (require('../app.json') as {
    expo: { version: string; ios: { buildNumber: string }; android: { versionCode: number } };
  }).expo;

  it('Android build.gradle matches version and versionCode', () => {
    const gradle = fs.readFileSync(path.join(root, 'android/app/build.gradle'), 'utf8');
    expect(gradle).toContain(`versionCode ${expo.android.versionCode}\n`);
    expect(gradle).toContain(`versionName "${expo.version}"`);
  });

  it('the iOS project matches version and build number', () => {
    const dir = fs.readdirSync(path.join(root, 'ios')).find((name) => name.endsWith('.xcodeproj'))!;
    const pbx = fs.readFileSync(path.join(root, 'ios', dir, 'project.pbxproj'), 'utf8');
    const marketing = [...pbx.matchAll(/MARKETING_VERSION = ([^;]+);/g)].map((m) => m[1]);
    const builds = [...pbx.matchAll(/CURRENT_PROJECT_VERSION = ([^;]+);/g)].map((m) => m[1]);
    expect(marketing.length).toBeGreaterThan(0);
    expect(new Set(marketing)).toEqual(new Set([expo.version]));
    expect(new Set(builds)).toEqual(new Set([expo.ios.buildNumber]));
  });

  it('the iOS Info.plist matches version and build number', () => {
    const app = fs.readdirSync(path.join(root, 'ios'))
      .find((name) => fs.existsSync(path.join(root, 'ios', name, 'Info.plist')))!;
    const plist = fs.readFileSync(path.join(root, 'ios', app, 'Info.plist'), 'utf8');
    const value = (key: string) => plist.match(new RegExp(`<key>${key}</key>\\s*<string>([^<]*)</string>`))?.[1];
    const resolve = (raw: string | undefined, setting: string, literal: string) =>
      raw === `$(${setting})` ? literal : raw;
    expect(resolve(value('CFBundleShortVersionString'), 'MARKETING_VERSION', expo.version)).toBe(expo.version);
    expect(resolve(value('CFBundleVersion'), 'CURRENT_PROJECT_VERSION', expo.ios.buildNumber)).toBe(expo.ios.buildNumber);
  });
});
