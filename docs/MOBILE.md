# Mobile — watch and control nzbd from iPhone, iPad, and Android

Companion to [USAGE.md](USAGE.md) (the daemon and browser UI) — this is how
to run the native mobile client in [`mobile/`](../mobile/).

The app is one Expo SDK 57 / React Native codebase for iOS, iPadOS, and
Android. It talks directly to nzbd's native `/api/v1` surface; there is no
web view and no service in between your device and the daemon.

> The app can use plain HTTP because many nzbd servers live on a trusted LAN.
> Plain HTTP exposes the NZB, queue metadata, and password or token to anyone
> who can observe that network. Use HTTPS through a trusted certificate when
> the connection leaves your LAN. The app does not bypass certificate checks.

## What the app controls

| Surface | Behavior |
|---|---|
| Queue state | Reads the full status and job snapshot from the SSE stream; polls every 5 seconds when that stream is stale |
| Whole queue | Pause and resume |
| One job | Pause · resume · move top/up/down/bottom · remove from the queue |
| Add | Picks an NZB or torrent file, or accepts a magnet / HTTP torrent URL |
| Discovery | Scans the local network for nzbd's `_nzbd._tcp` DNS-SD service and fills the server address when selected |
| History | Lists completed, failed, and removed jobs with size, health, completion time, pickup state, and retained job details |
| Logs | Shows the recent system and job log, refreshes every 4 seconds, and can include per-file detail |
| Providers | Shows the live rate and blocked state of every configured news server |
| Authentication | Basic username/password or bearer token, stored in iOS Keychain or Android Keystore |
| Display | Classic/Plex/Theater navigation, ten color schemes, and independent Auto/Light/Dark appearance, stored on the device |

Removing a job leaves downloaded files in place. When the daemon can park the
job in history, the success message says it can be restored there. The mobile
History tab is read-only; use the browser UI for restore, hide, record delete,
and delete-files actions. Mobile requests carry `X-Nzbd-Role: operator`, so
opening History does not claim that the app imported a job or replace its
`picked_up_by` attribution.

## Queue activity and torrent seeding

The native queue uses the same lifecycle rules as the web: Downloading,
Fetching NZB, Fetching torrent metadata, Checking torrent files, the NZB
post-processing stages, Seeding, Completed, and Waiting. Completed means a
verified torrent is retained with seeding stopped; it does not mean an import
into a library or a move to History. Checking, missing files, and failures take
precedence over stale readiness.

Tap Seeding, Completed, or Waiting to collapse it. Counts and transfer totals
continue updating while cards are hidden. The device remembers these display
choices across connections and restarts; all groups initially open. A failed
preference read defaults to expanded groups, and a failed save still keeps
your choice for that session.

Expand a torrent card for its upload, ratio, peer, readiness, and seed-duration
facts. Seeding options can copy category/global defaults, stop after download,
keep seeding, or stop at the first ratio/time limit reached. Saving a policy
does not restart a stopped torrent: use Start seeding separately. A reached
policy offers Seeding options directly. If a server refuses a restart with an
ambiguous 404, the app refreshes that job before deciding whether to open
options or report that it has left the queue.

Missing files offers Re-download missing files. Failed torrents offer removal
with the existing keep/delete-files choice. Disk holds explain automatic
recovery when space is available; a resume button is not a disk-space remedy.
The new sections do not expose NZB stage timers or health for torrents, nor
queue-move and priority controls for seeds. Older daemons can omit lifecycle
fields; the app falls back to generic status. Unknown states remain visible,
and an unsupported policy endpoint reports an error without breaking the queue.

[The parity status page](MOBILE_QUEUE_PARITY_STATUS.md) separates implementation
and test evidence from iOS/Android distribution evidence.

## Run it locally

Expo SDK 57 requires Node.js 22.13 or newer, iOS 16.4 or newer, or Android 7
or newer. Xcode is needed for an iOS build; Android Studio and an Android SDK
are needed for Android.

```bash
cd mobile                 # enter the React Native package
npm install               # install the lockfile-pinned dependencies
npm run ios               # build and run the iPhone/iPad simulator app
npm run android           # build and run an Android emulator or device
npm run ios:device        # install a self-contained build on an Apple device
```

`npm run ios:device` uses Xcode's Release configuration so the installed app
embeds its JavaScript bundle and keeps working when this Mac and Metro are
offline. Xcode and `expo run:ios` otherwise default to Debug; a Debug build is
only for active development and stops at the splash screen with “No script
URL provided” whenever Metro is not running.

The `ios/` and `android/` projects are committed so they open directly in
Xcode and Android Studio. `app.json` plus its Expo config plugins are the
authoritative configuration inputs. After changing either, run
`npx expo prebuild --clean`, review the regenerated native-project diff, and
commit it with the configuration change.

On the first screen, select a server under **Nearby nzbd** or enter an address
the device itself can reach. A bare hostname or IP address gains nzbd's
default port automatically, so `nuc3` becomes `http://nuc3:6789`. Accept the
Local Network prompt on iOS so the list can populate. Do not enter `localhost`
unless nzbd is running on the phone. Add a bearer token, or the configured
control username and password. Open servers need neither.

The daemon advertises only after it has a reachable listener. A default
loopback-only server is intentionally invisible. Enable LAN listening and
leave discovery on in `nzbd.toml`:

```toml
[api]
bind = "0.0.0.0:6789"
discovery = true
```

Discovery stays inside the local multicast domain; it does not cross a VPN,
router, or VLAN unless that network explicitly reflects mDNS. The advertised
TXT record contains the API path, TLS flag, accepted authentication modes,
and server version—not credentials or queue contents. Manual entry remains
available when multicast is blocked. Android emulators do not reliably pass
mDNS traffic, so verify discovery on a physical Android device before release.

A daemon in a bridged Docker container cannot send multicast onto the host's
physical LAN. Use the `nzbd-discovery` host-network companion in
[`examples/docker-compose/docker-compose.yml`](../examples/docker-compose/docker-compose.yml),
or run `nzbd advertise --name <host> --port 6789` directly on the host.

## Verify a change

```bash
cd mobile
npm run typecheck          # strict TypeScript contract check
npm test                   # API formatting and incremental SSE parser tests
npm run doctor             # Expo dependency and app-config compatibility
npm run export             # bundle every declared platform without a device
```

`npm run export` is the closest fast check to a native compile: Metro must
resolve every screen, native module, and asset. Run `npm run ios` and
`npm run android` before a release because a JavaScript bundle cannot prove
Xcode signing, Android SDK setup, local-network permissions, or document
provider behavior on a real device.

## Build a signed Android release

The committed development keystore signs debug builds only. Android release
artifact tasks fail closed unless all four upload-key settings are supplied;
the build never silently falls back to the public debug key.

Generate and back up a private upload key outside the repository. For example:

```bash
keytool -genkeypair -v -storetype PKCS12 \
  -keystore /private/path/nzbd-upload-key.p12 \
  -alias nzbd-upload -keyalg RSA -keysize 2048 -validity 10000
```

Put the settings in `~/.gradle/gradle.properties`, which must remain outside
version control and readable only by your account:

```properties
NZBD_ANDROID_STORE_FILE=/private/path/nzbd-upload-key.p12
NZBD_ANDROID_STORE_PASSWORD=replace-with-the-store-password
NZBD_ANDROID_KEY_ALIAS=nzbd-upload
NZBD_ANDROID_KEY_PASSWORD=replace-with-the-key-password
```

Then build the Play Store bundle:

```bash
chmod 600 ~/.gradle/gradle.properties
cd mobile/android
./gradlew bundleRelease
```

The same four names can be supplied as environment variables in a protected
build environment. Treat this key as the Google Play App Signing upload key:
keep an offline backup, restrict access, and do not commit either the key or
its passwords.

## iPad is a first-class layout

The iOS target declares tablet support and leaves full-screen mode off, so it
participates in iPad Split View and Stage Manager. At 820 points and wider,
queue rows stay in the main column while rates and providers move into a
fixed sidebar. Below that width the same information stacks for phone and
narrow split-screen windows. Rotation is not locked.

## Display choices preserve the original as Classic

Open **Display** from the dashboard header or connection screen. Layout,
color scheme, and appearance are separate choices:

- **Classic** is the native layout nzbd shipped before the selector and stays
  the default. **Plex** pins the navigation strip on wide screens and moves it
  to the bottom on phones. **Theater** turns the primary tabs into a wide top
  deck.
- **Color scheme** offers Classic, Terminal, noirr, Amber, Giallo, Silver,
  Void, VHS, Paper, and Tide. Classic keeps the previous native colors.
- **Appearance** defaults to Auto, follows the device, and can be held on
  Light or Dark. Void and VHS are midnight-only, so they remain dark even if
  Light is selected.

All three settings are stored in iOS Keychain or Android Keystore. Existing
theme choices migrate through the original `nzbd.theme.v1` key; an upgrade
therefore changes neither appearance nor navigation until you choose a new
option.

## Network and certificate behavior

`app.json` opts Android into cleartext traffic and gives iOS an
`NSBonjourServices` declaration, a local-network usage string, and an App
Transport Security exception. Those settings exist because Bonjour access
must be declared and a user-entered private IP cannot be enumerated as an ATS
exception at build time. They permit discovery and HTTP; they do not make the
connection private.

Expo SDK 57 currently targets Android API 36, where the normal `INTERNET`
permission still grants local-network access. Android 17 enforces a separate
permission for apps once they target API 37. When this project raises its
target SDK, add the `ACCESS_LOCAL_NETWORK` runtime flow or move discovery to
Android's user-mediated NSD picker at the same time; do not silently ship the
target bump with discovery broken. See Android's
[local-network permission guide](https://developer.android.com/privacy-and-security/local-network-permission).

HTTPS still uses the platform trust store. A self-generated nzbd certificate
must be trusted on the device before the app can connect, or nzbd should sit
behind a reverse proxy with a certificate the device already trusts. There
is deliberately no "ignore certificate errors" switch: that would also hide
an intercepted connection.

## Deliberate limits

- **No remote-access relay.** You provide network reachability with LAN,
  VPN, or a reverse proxy; the app never publishes nzbd to the internet.
- **No background transfer or notifications.** Queue watching is live while
  the app is active. The daemon keeps downloading when it is closed.
- **No settings or history editor.** History and logs are visible, but the
  browser UI remains the administration surface for configuration and
  destructive history actions.
- **One saved server.** Credentials and the current origin are one small
  secure-store record. A server switch replaces that record.

## Code map

| Path | Job |
|---|---|
| [`mobile/App.tsx`](../mobile/App.tsx) | Loads the saved connection and selects setup or dashboard |
| [`mobile/src/api/`](../mobile/src/api/) | Native API types, authenticated requests, status formatting, and SSE parsing |
| [`mobile/src/discovery/`](../mobile/src/discovery/) | DNS-SD lifecycle plus safe conversion of advertised hosts into connection URLs |
| [`mobile/src/hooks/useNzbd.ts`](../mobile/src/hooks/useNzbd.ts) | Owns live-stream reconnects, stale polling, and mutations |
| [`mobile/src/screens/`](../mobile/src/screens/) | Connection, phone, tablet, queue-control, and NZB-picker UI |
| [`mobile/__tests__/`](../mobile/__tests__/) | Wire-shape and chunk-boundary regression tests |
