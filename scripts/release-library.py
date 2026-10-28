#!/usr/bin/env python3
"""Publish an exact library checkout with private, bounded release receipts.

See the repository README. This does not compile or replace consumer testing.
The plan supplies existing destinations and local paths; none are embedded here.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tarfile
import tempfile
import time
import tomllib
from urllib.parse import urlsplit
import urllib.error
import urllib.request

HELPER_SHA256 = "d60f75bc324e128b20b0bd1a1ba76b87c3161cbbb45cb656c79712bd9ee22ec4"
# Preinstalled git-remote-htree 0.2.150, aarch64-apple-darwin. Archive pin:
HELPER_ARCHIVE_SHA256 = "ea575ecfb06d5dc461010fd577a6d3a7397f0bd82dc64fdddf0261d0a11706ad"
TOOLCHAIN = "1.99.0"


def sha(data):
    return hashlib.sha256(data).hexdigest()


class Release:
    def __init__(self, plan, output, mode):
        self.plan, self.out, self.mode = plan, output, mode
        self.root = Path(plan["root"]).resolve()
        self.workspace = self.root / plan["workspace"]
        self.start = time.monotonic()
        self.phases = []
        self.interrupted = False
        self.initial = self.minimum = self.free()
        self.env = dict(os.environ, RUSTUP_TOOLCHAIN=TOOLCHAIN,
                        CARGO_TARGET_DIR=plan["target"], CARGO_BUILD_JOBS="1",
                        CARGO_INCREMENTAL="0", RUSTC_WRAPPER="")
        self.out.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.receipt = self.out / (mode + "-closure.json")
        assert not self.receipt.exists(), "Inspect the existing attempt before continuing"
        for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self.interrupt)
        assert re.fullmatch(r"[0-9a-f]{40}", plan["head"])
        assert self.initial >= plan["entry_bytes"], "Free-space entry gate"

    def interrupt(self, _signal, _frame):
        self.interrupted = True

    def free(self):
        fs = os.statvfs(self.root)
        return fs.f_bavail * fs.f_frsize

    def save(self, name, value):
        (self.out / (name + ".json")).write_text(json.dumps(value, indent=2) + "\n")

    def guard(self):
        available = self.free()
        self.minimum = min(self.minimum, available)
        assert not self.interrupted, "Interrupted"
        assert time.monotonic() - self.start < self.plan["seconds"], "Time budget"
        assert available >= self.plan["floor_bytes"], "Protected free-space floor"
        assert self.initial - available <= self.plan["growth_bytes"], "Space growth budget"

    @staticmethod
    def exists(group):
        try:
            os.killpg(group, 0)
            return True
        except ProcessLookupError:
            return False

    def run(self, name, args, cwd=None, env=None):
        self.guard()
        row = {"name": name, "command": args, "started": time.monotonic() - self.start}
        process = None
        logpath = self.out / (self.mode + "-" + name + ".log")
        try:
            with logpath.open("xb") as log:
                process = subprocess.Popen(args, cwd=cwd or self.root, env=env or self.env,
                                           stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                row["group"] = process.pid
                self.save(self.mode + "-current", row)
                while process.poll() is None:
                    self.guard()
                    assert time.monotonic() - self.start - row["started"] < self.plan["phase_seconds"], "Phase budget"
                    time.sleep(.2)
        finally:
            if process is not None:
                for sig, grace in ((signal.SIGTERM, 5), (signal.SIGKILL, 2)):
                    process.poll()
                    if not self.exists(process.pid):
                        break
                    try:
                        os.killpg(process.pid, sig)
                    except ProcessLookupError:
                        break
                    until = time.monotonic() + grace
                    while self.exists(process.pid) and time.monotonic() < until:
                        process.poll()
                        time.sleep(.05)
                process.wait(timeout=2)
                row.update(exit_code=process.returncode, reaped=not self.exists(process.pid))
            row["ended"] = time.monotonic() - self.start
            self.phases.append(row)
            self.save(self.mode + "-phases", self.phases)
        assert row.get("exit_code") == 0 and row.get("reaped"), name + " failed; inspect its log"
        return logpath.read_text()

    def clean(self, label):
        assert self.run(label + "-head", ["git", "rev-parse", "HEAD"]).strip() == self.plan["head"]
        assert not self.run(label + "-status", ["git", "status", "--porcelain"]).strip(), "Dirty source"

    def helper(self):
        directory = Path(self.plan["helper_directory"])
        assert sha((directory / "git-remote-htree").read_bytes()) == HELPER_SHA256
        return dict(self.env, PATH=str(directory) + os.pathsep + self.env["PATH"],
                    HTREE_PREFER_LOCAL_DAEMON="false", HTREE_LOCAL_DAEMON_ONLY="false", NOSTR_PREFER_LOCAL="0")

    @staticmethod
    def refs(text):
        return {line.split()[1]: line.split()[0] for line in text.splitlines()
                if len(line.split()) == 2 and (line.split()[1] == "HEAD" or line.split()[1].startswith("refs/"))}

    def source(self):
        self.clean("before")
        env = self.helper()
        source = self.plan["source"]
        configured = self.run("remote", ["git", "remote", "get-url", source["remote"]]).strip()
        assert configured == source["existing_url"]
        public, existing = urlsplit(source["public_url"]), urlsplit(configured)
        assert public.scheme == existing.scheme == "htree"
        assert re.fullmatch(r"npub1[023456789acdefghjklmnpqrstuvwxyz]{58}", public.netloc)
        assert public.path == existing.path and not any((public.query, public.fragment, existing.query, existing.fragment))
        before = self.refs(self.run("public-before", ["git", "ls-remote", "--", source["public_url"]], env=env))
        alias = self.refs(self.run("alias-before", ["git", "ls-remote", "--", source["remote"]], env=env))
        assert before and alias == before, "Existing source ownership differs"
        ref = "refs/tags/haps-source/" + self.plan["head"]
        assert ref not in before or before[ref] == self.plan["head"]
        self.run("push", ["git", "-c", "push.followTags=false", "push", "--", source["remote"], self.plan["head"] + ":" + ref], env=env)
        after = self.refs(self.run("public-after", ["git", "ls-remote", "--", source["public_url"]], env=env))
        assert after.get(ref) == self.plan["head"] and all(after.get(k) == v for k, v in before.items())
        cold = Path(tempfile.mkdtemp(prefix="cold-source-", dir=self.out))
        cold_env = {k: v for k, v in env.items() if not k.startswith(("HTREE_", "GIT_", "NOSTR_"))}
        cold_env.update(HTREE_CONFIG_DIR=str(cold / "config"), HTREE_DATA_DIR=str(cold / "data"),
                        HTREE_PREFER_LOCAL_DAEMON="false", HTREE_LOCAL_DAEMON_ONLY="false", NOSTR_PREFER_LOCAL="0",
                        GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_NO_REPLACE_OBJECTS="1", GIT_TERMINAL_PROMPT="0")
        repo = cold / "repo.git"
        git = ["git", "-c", "protocol.ext.allow=never", "-C", str(repo)]
        self.run("init", ["git", "init", "--bare", str(repo)], env=cold_env)
        self.run("fetch", [*git, "fetch", "--no-tags", "--no-write-fetch-head", "--", source["public_url"], ref + ":" + ref], env=cold_env)
        assert self.run("revision", [*git, "rev-parse", "--verify", ref + "^{commit}"], env=cold_env).strip() == self.plan["head"]
        assert self.run("shallow", [*git, "rev-parse", "--is-shallow-repository"], env=cold_env).strip() == "false"
        assert not (repo / "objects/info/alternates").exists()
        objects = self.run("objects", [*git, "rev-list", "--objects", "--missing=error", self.plan["head"], "--"], env=cold_env)
        assert objects.strip()
        self.run("fsck", [*git, "fsck", "--full", "--strict", "--no-reflogs", self.plan["head"]], env=cold_env)
        self.clean("after")
        return {"head": self.plan["head"], "immutable_ref": ref, "helper_sha256": HELPER_SHA256,
                "nonshallow": True, "empty_cache": True, "local_daemon": False, "strict_fsck": True,
                "object_count": len(objects.splitlines()), "object_manifest_sha256": sha(objects.encode())}

    def request(self, url, missing=False):
        self.guard()
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "library-release-verification"})
            with urllib.request.urlopen(request, timeout=20) as response:
                data = response.read(25 * 1024 * 1024 + 1)
                assert len(data) <= 25 * 1024 * 1024
                self.guard()
                return data
        except urllib.error.HTTPError as error:
            if missing and error.code == 404:
                return None
            raise

    def package(self):
        self.clean("before")
        source = json.loads(Path(self.plan["source_receipt"]).read_text())
        assert source["passed"] and source["result"]["head"] == self.plan["head"]
        assert source["result"]["empty_cache"] and source["result"]["strict_fsck"]
        name, version = self.plan["crate"], self.plan["version"]
        api = "https://crates.io/api/v1/crates/" + name + "/" + version
        assert self.request(api, missing=True) is None, "Version exists; verify existing upload before continuing"
        output = self.run("package", ["cargo", "+" + TOOLCHAIN, "package", "--locked", "--no-verify", "-p", name], cwd=self.workspace)
        assert "Compiling " not in output
        path = Path(self.plan["target"]) / "package" / (name + "-" + version + ".crate")
        archive = path.read_bytes()
        prefix = name + "-" + version + "/"
        verified, seen = [], set()
        with tarfile.open(fileobj=io.BytesIO(archive)) as packed:
            vcs = json.load(packed.extractfile(prefix + ".cargo_vcs_info.json"))
            assert vcs["git"]["sha1"] == self.plan["head"] and not vcs["git"].get("dirty", False)
            assert vcs["path_in_vcs"] == self.plan["crate_path"]
            manifest = tomllib.loads(packed.extractfile(prefix + "Cargo.toml").read().decode())
            assert manifest["package"]["name"] == name and manifest["package"]["version"] == version
            assert "patch" not in manifest and "replace" not in manifest
            for dependency, requirement in self.plan.get("dependency_requirements", {}).items():
                assert manifest["dependencies"][dependency]["version"] == requirement
            workspace_lock = tomllib.loads((self.workspace / "Cargo.lock").read_text())["package"]
            expected = {(p["name"], p["version"]): p.get("checksum") for p in workspace_lock}
            package_lock = tomllib.loads(packed.extractfile(prefix + "Cargo.lock").read().decode())["package"]
            for package in package_lock:
                key = (package["name"], package["version"])
                assert key in expected, "Unpinned package: " + str(key)
                if expected[key] is not None:
                    assert expected[key] == package.get("checksum"), key
                pinned = self.plan.get("registry_checksums", {}).get(package["name"] + "@" + package["version"])
                if pinned is not None:
                    assert package.get("checksum") == pinned, key
            for member in packed.getmembers():
                assert member.name.startswith(prefix) and member.name not in seen
                seen.add(member.name)
                relative = member.name[len(prefix):]
                assert not Path(relative).is_absolute() and ".." not in Path(relative).parts
                assert member.isfile(), "Unexpected package entry type"
                if relative in ("Cargo.toml", "Cargo.lock", ".cargo_vcs_info.json"):
                    continue
                source_path = self.root / self.plan["crate_path"] / ("Cargo.toml" if relative == "Cargo.toml.orig" else relative)
                assert source_path.is_file() and source_path.read_bytes() == packed.extractfile(member).read(), relative
                assert bool(source_path.stat().st_mode & 0o111) == bool(member.mode & 0o111), relative
                verified.append(relative)
        assert verified
        proof = {"name": name, "version": version, "head": self.plan["head"], "sha256": sha(archive),
                 "bytes": len(archive), "source_files": verified, "locked_packages": len(package_lock)}
        (self.out / path.name).write_bytes(archive)
        self.save("package-proof", proof)
        self.clean("publish")
        output = self.run("publish", ["cargo", "+" + TOOLCHAIN, "publish", "--locked", "--no-verify", "-p", name], cwd=self.workspace)
        assert "Compiling " not in output and sha(path.read_bytes()) == proof["sha256"]
        metadata = None
        for delay in (0, 2, 4, 8, 16):
            if delay:
                time.sleep(delay)
            raw = self.request(api, missing=True)
            if raw is not None:
                metadata = json.loads(raw)["version"]
                break
        assert metadata is not None, "Visibility pending; verify instead of republishing"
        assert metadata["checksum"] == proof["sha256"] and not metadata["yanked"]
        remote = self.request("https://static.crates.io/crates/" + name + "/" + path.name)
        assert remote == archive
        self.clean("after")
        return dict(proof, registry_byte_equal=True, yanked=False)

    def refs_release(self):
        self.clean("before")
        package = json.loads(Path(self.plan["package_receipt"]).read_text())
        assert package["passed"] and package["result"]["head"] == self.plan["head"]
        tag = self.plan["tag"]
        existing = self.run("local-tag", ["git", "tag", "--list", tag]).strip()
        if not existing:
            self.run("tag", ["git", "tag", "-a", tag, "-m", "Release " + self.plan["crate"] + " " + self.plan["version"], self.plan["head"]])
        assert self.run("peel", ["git", "rev-parse", tag + "^{commit}"]).strip() == self.plan["head"]
        env = self.helper()
        refs = [tag]
        if self.plan.get("push_master"):
            assert self.run("branch", ["git", "branch", "--show-current"]).strip() == "master"
            refs.append("master")
        self.run("push", ["git", "push", "origin", *refs], env=env)
        observed = self.refs(self.run("readback", ["git", "ls-remote", "origin", "refs/tags/" + tag, "refs/heads/master"], env=env))
        assert observed["refs/tags/" + tag] == self.run("tag-object", ["git", "rev-parse", tag]).strip()
        if self.plan.get("push_master"):
            assert observed["refs/heads/master"] == self.plan["head"]
        self.clean("after")
        return {"head": self.plan["head"], "refs": observed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("source", "package", "refs"))
    parser.add_argument("plan", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    plan_bytes = args.plan.read_bytes()
    release = Release(json.loads(plan_bytes), args.output.resolve(), args.mode)
    result, failure = None, None
    try:
        result = {"source": release.source, "package": release.package, "refs": release.refs_release}[args.mode]()
    except Exception as error:
        failure = str(error)
    receipt = {"passed": failure is None, "failure": failure, "result": result,
               "plan_sha256": sha(plan_bytes), "script_sha256": sha(Path(__file__).read_bytes()),
               "elapsed_seconds": time.monotonic() - release.start, "initial_free_bytes": release.initial,
               "minimum_free_bytes": release.minimum, "final_free_bytes": release.free(), "phases": release.phases}
    release.save(args.mode + "-closure", receipt)
    print(json.dumps({"passed": receipt["passed"], "failure": failure, "receipt": str(release.receipt)}), flush=True)
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
