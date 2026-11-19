# Smart Meter

Smart Meter is a two-region IoT telemetry company that runs on QEMU hosts. SREGym's `usage_reporting` problem (inspired by Datadog's March 2023 outage) revives a saved Smart Meter world. The agent then works through `companyctl` and the company's own endpoints.

**Per region:**
- a Kubernetes 1.26 cluster with Cilium 1.13.2;
- EMQX 5.0.26 and gateways;
- Kafka 3.4 (ZooKeeper in the US, KRaft in the EU);
- a PostgreSQL ledger and writer;
- usage reporting;
- partner statements mirrored by MirrorMaker.

**Fleet-wide:**
- a Gitea platform repository holding the fleet contract;
- a platform role, a package channel, a capacity controller and an auto-healer;
- 2,000 device clients on a private helper host.

## Layout

| Path | Contents |
|---|---|
| `world/` | World runtime: VMs, provider, serial console tools, saved-world revive, company software pushed to hosts |
| `scenarios/usage_reporting/` | How this problem's world is built: hosts, clusters, company services, history, seal |
| `grader/` | Mitigation grader; pass rules in [`grader/RUBRIC.md`](grader/RUBRIC.md) |
| `inputs/` | Pinned upstream inputs (`inputs.json`, sha256 for every byte) and `fetch.py` |

## Host requirements

- Linux x86_64 with KVM (`/dev/kvm` readable and writable by the conductor user). The guests use QEMU's generic `Westmere-v2` processor model, so the saved world wakes on any Intel processor with its features (Westmere, 2010, or newer, with AES-NI); QEMU refuses to start on a host that lacks one. AMD hosts are untested.
- QEMU 8.2 or newer (`qemu-system-x86_64`, `qemu-img`; the world is saved with machine type `pc-q35-8.2`), a systemd user session, SSH, gzip, zstd, Docker (for the agent container) and passwordless `sudo` (the grader reads frozen disks in a private mount namespace).
- The world allocates 20 GiB of guest RAM across 9 hosts and runs in a 10-CPU, 28 GiB slice. A 32 GB, 12-CPU host is enough: on a 48 GB host the whole machine peaked at 22.3 GiB in use during a run and its grading.
- Disk: about 60 GB free for the 10.2 GB package, the 18 GB installed world and 30 GiB per run (a build used 17 GB for the world, plus 2.6 GB of inputs).
- One world per installation runs at a time.

`bash setup_host.sh` installs the packages on apt-based systems, adds you to the `kvm` and `docker` groups and checks that this host can wake the saved guests (processor model, machine type, memory).

## Use a saved world

SREGym's first run of `usage_reporting` downloads and installs the published world itself. To install it yourself:

1. Download the saved world bundle as shown in "Install the world" in SREGym's `docs/usage-reporting.md`.
2. Install it:

   ```sh
   python3 world/bundle.py install ~/usage-reporting-world/smart-meter-usage-reporting.tar.zst
   ```

   This unpacks the bundle into `world/vm/states/usage-reporting` (and the guests' SSH key, boot seeds and the Ubuntu base image into `world/`), checks every file against the bundle's SHA256SUMS and points the saved disks at this folder. A run needs no other input.
3. Run the problem from SREGym:

   ```sh
   python main.py --problem usage_reporting --agent claudecode --stages diagnosis mitigation --agent-timeout 1800
   ```

   SREGym uses this folder in its `SREGym-applications` checkout.

## Build a world from source (about 4.5 hours)

```sh
python3 inputs/fetch.py --mirror "$SMART_METER_INPUTS_MIRROR"
python3 scenarios/usage_reporting/build.py
```

The build has four stages:
1. It creates fresh hosts from the Ubuntu 22.04 cloud image, installs both clusters and the company services, and starts device traffic.
2. It replays the incident's history in real time: a systemd security update, then a Cilium configuration change. The fleet's own automation (role runs, the healer) reacts as it would.
3. It seals the world once the fleet settles.
4. It saves the world as `world/vm/states/usage-reporting`, with a census of where every acknowledged reading lives.

The history step fixes the order in which hosts fail, so every build yields the same incident.

To share a world you built, export it as one bundle (a `.zst` name compresses it with zstd, as the published bundle is; `-` streams a plain tar to stdout):

```sh
python3 world/bundle.py export usage-reporting smart-meter-usage-reporting.tar.zst
```

## Grading

Diagnosis uses SREGym's standard LLM judge. Mitigation is graded from the end state only; the six checks are in [`grader/RUBRIC.md`](grader/RUBRIC.md). `grader/FREEZE.json` pins the files that define a grade.

## SREGym integration

- SREGym's adapter is `sregym/service/apps/smart_meter` (registered as `smart-meter`), and the problem is `usage_reporting`.
- SREGym owns the run: revive, the agent container and company endpoints, the cutoff at the end of the run (D), grading and teardown.
- The adapter runs `world/sregym_lifecycle.py` (`ready`, `freeze`, `teardown`) to ready, freeze and tear down a revived world; its tests are `world/test_sregym_lifecycle.py`.
- See the [usage reporting guide](https://github.com/SREGym/SREGym/blob/main/docs/usage-reporting.md) for supported execution and evaluation.
- SREGym keeps the diagnosis answer keys outside this folder.

## Provenance

Smart Meter is original to SREGym. Its incident follows Datadog's public postmortem of the March 8, 2023 outage: an automatic systemd security update made systemd-networkd remove Cilium's policy routing rules.

The upstream inputs keep their own licenses. They are pinned in `inputs/inputs.json`:
- the Ubuntu cloud image and packages;
- Kubernetes and Cilium;
- EMQX, Kafka and PostgreSQL;
- Helm.

Smart Meter's own code uses MIT.
