# -*- coding: utf-8 -*-
import logging

from kazoo.client import KazooClient

logger = logging.getLogger(__name__)

class ZKOffsetManager(object):
    """Zookeeper offset manager for using Zookeeper to manage offsets fetching and committing
    Args:
        config (dict): configuration
            'hosts' (str) : hosts list of Zookeeper cluster seperated by ','
                ex. '192.168.67.65:2181,192.168.67.67:2181'
            'znode' (str) : znode path, the name space consists of data registers
    """
    def __init__(self, config, partitions):
        self._partitions = partitions
        self._znode = config['znode']
        self._client = KazooClient(config['hosts'])
        self._client.start()
        self._check_path()

    def _get_zpath(self, partition):
        path = '{}/{}'.format(self._znode, partition)
        return path

    def _check_path(self):
        self._client.ensure_path(self._znode)
        for p in self._partitions:
            path = self._get_zpath(p)
            if not self._client.exists(path):
                self._client.create(path, '')

    def get_offsets(self):
        """Get offsets of each partitions
        Args:
            None
        Returns:
            dict : offsets of each partitions { pid (int) : offset (int) }
        """
        offsets = {p: self._get_single_offset(p) for p in self._partitions}
        logger.info("get committed offsets %s", offsets)
        return offsets

    def _get_single_offset(self, p):
        offset, znode_stat = self._client.get(self._get_zpath(p))
        return int(offset)

    def set_offsets(self, offsets):
        """Set offsets of each partitions
        Args:
            offsets (dict) : offsets of each partitions { pid (int) : offset (int) }
        Returns:
            None
        """
        logger.info("set committed offsets %s", offsets)
        for p, o in offsets.iteritems():
            self._set_single_offset(p, o)

    def _set_single_offset(self, p, offset):
        self._client.set(self._get_zpath(p), str(offset))

    def stop(self):
        """Stop manager
        Args:
            None
        Returns:
            None
        """
        self._client.stop()
