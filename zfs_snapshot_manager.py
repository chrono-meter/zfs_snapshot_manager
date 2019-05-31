#!/usr/bin/env python3
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import re
import math
import itertools
import distutils.util
import logging
import pprint


logger = logging.getLogger(__name__)


class SnapshotCleanupManager:
    snapshot_cleanup_rules = [
        # {'period': timedelta(hours=1), 'keep': 4},
        # todo: json format ex. {'period': {'hours': 1}, 'keep': 4}
        {'period': timedelta(hours=3), 'keep': 3},
        {'period': timedelta(hours=24), 'keep': 4},
        {'period': timedelta(days=7), 'keep': 7},
        {'period': timedelta(days=28), 'keep': 4},
        {'period': timedelta(days=365), 'keep': 12},
        {'period': timedelta(days=3650), 'keep': 10},
    ]

    def get_snapshots(self):
        raise NotImplementedError('get_snapshots')

    def get_snapshot_timestamp(self, snapshot):
        raise NotImplementedError('get_snapshot_timestamp')

    def remove_snapshot(self, snapshot):
        raise NotImplementedError('remove_snapshot')

    def cleanup_snapshots(self, now=None):
        if now is None:
            now = datetime.now()

        snapshots = [{'object': x, 'timestamp': self.get_snapshot_timestamp(x)} for x in self.get_snapshots()]
        snapshots.sort(key=lambda x: x['timestamp'])  # old is left

        for rule in sorted(self.snapshot_cleanup_rules, key=lambda x: x['period']):
            assert rule['period'].total_seconds() > 0 and rule['keep'] > 0, 'invalid rule'

            candidate_groups = {now - rule['period'] / rule['keep'] * (i + 1): [] for i in range(rule['keep'])}
            remaining = []
            for snapshot in snapshots:
                for threshold in sorted(candidate_groups, reverse=True):
                    if snapshot['timestamp'] >= threshold:
                        candidate_groups[threshold].append(snapshot)
                        break
                else:
                    remaining.append(snapshot)

            snapshots = remaining

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(pprint.pformat({
                    'now': now,
                    'rule': rule,
                    'candidate_groups': candidate_groups,
                }))

            candidate_groups = candidate_groups.values()

            while sum(len(x) for x in candidate_groups) > rule['keep']:
                candidate_groups = list(filter(None, candidate_groups))
                candidate_groups.sort(key=lambda x: (len(x), x[-1]['timestamp']))
                self.remove_snapshot(candidate_groups[-1].pop(-1)['object'])  # old is important
                # self.remove_snapshot(candidate_groups[-1].pop(math.floor(len(candidate_groups[-1]) / 2))['object'])
                # self.remove_snapshot(candidate_groups[-1].pop(0)['object'])  # 古いものを消すと脱出できない


class ZfsCommand:
    zfs_path = '/sbin/zfs'

    def _run(self, *args, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, universal_newlines=True, **kwargs):
        try:
            return subprocess.run(*args, check=check, stdout=stdout, stderr=stderr, close_fds=close_fds, universal_newlines=universal_newlines, **kwargs)
        except subprocess.CalledProcessError as e:
            logger.debug(e.stdout + e.stderr, exc_info=True)
            raise

    def zfs_list_name(self):
        p = self._run([self.zfs_path, 'list', '-H', '-o', 'name'])
        return p.stdout.splitlines()

    def zfs_get(self, name: str, property: str):
        p = self._run([self.zfs_path, 'get', '-H', '-o', 'value', property, name])
        result = p.stdout.rstrip()

        handler = getattr(self, 'on_zfs_get_' + property, None)
        if callable(handler):
            result = handler(result)

        return result

    def on_zfs_get_creation(self, result: str):
        """@link https://github.com/zfsonlinux/zfs/search?l=C&q=strftime
        """
        # "%a %b %e %k:%M %Y"  # Python doesn't support '%k', use '%H'.
        return datetime.strptime(result, "%a %b %d %H:%M %Y")

    def zfs_list_snapshot(self, name: str, depth: int=1):
        p = self._run([self.zfs_path, 'list', '-H', '-d', str(depth), '-t', 'snapshot', '-o', 'name', name])
        return p.stdout.splitlines()

    def zfs_snapshot(self, name):
        self._run([self.zfs_path, 'snapshot', name])

    def zfs_destroy(self, name):
        self._run([self.zfs_path, 'destroy', name])


class ZfsSnapshotManager(ZfsCommand, SnapshotCleanupManager):
    # todo: including '@' in zfs snapshot name is not supported
    # @link https://github.com/samba-team/samba/blob/master/source3/include/smb.h
    GMT_FORMAT = "@GMT-%Y.%m.%d-%H.%M.%S"  # datetime.strptime("@GMT-2019.05.20-01.23.45", "@GMT-%Y.%m.%d-%H.%M.%S") --> datetime(2019, 5, 20, 1, 23, 45)
    # todo: Samba integration (snapshot name shadow:format from smb.conf or testparm)
    # todo: cleanup_snapshots by quota

    def __init__(self, dataset):
        self.dataset = dataset

    def get_snapshots(self):
        return self.zfs_list_snapshot(self.dataset)

    def get_snapshot_timestamp(self, snapshot):
        return self.zfs_get(snapshot, 'creation')

    def remove_snapshot(self, snapshot):
        # todo: check hold
        self.zfs_destroy(snapshot)
        logger.info('Snapshot %s is removed.', snapshot)

    def create_snapshot(self):
        snapshot = self.dataset + datetime.utcnow().strftime(self.GMT_FORMAT)
        self.zfs_snapshot(snapshot)
        logger.info('Snapshot %s is created.', snapshot)


class App(ZfsCommand):
    auto_snapshot_enabled_key = 'com.sun:auto-snapshot'

    def get_target_dataset(self):
        for dataset in self.zfs_list_name():
            v = self.zfs_get(dataset, self.auto_snapshot_enabled_key)

            if v == '-' or not distutils.util.strtobool(v):
                continue

            yield dataset

    def run(self):
        for dataset in self.get_target_dataset():
            manager = ZfsSnapshotManager(dataset)
            # todo: --no-snapshot
            manager.create_snapshot()
            manager.cleanup_snapshots()


class TestSnapshotManager(SnapshotCleanupManager):

    def get_snapshots(self):
        return self.snapshots

    def get_snapshot_timestamp(self, snapshot):
        return snapshot

    def remove_snapshot(self, snapshot):
        self.snapshots.remove(snapshot)

    def run(self):
        logger.setLevel(logging.WARNING)
        self.snapshots = []
        now = datetime.now()
        begin = now - timedelta(days=365 * 10, seconds=-100)
        snapshot_period = timedelta(minutes=60)

        t = begin
        while t < now:
            self.snapshots.append(t)
            if t >= now - snapshot_period:  # last one
                logger.setLevel(logging.DEBUG)
            self.cleanup_snapshots()
            t += snapshot_period

        for x in self.snapshots: print(x)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.WARNING)
    manager = App()
    # manager = TestSnapshotManager()
    manager.run()
