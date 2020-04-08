PATTERN = 'rqueue_{}:{}'


class RQueue(object):
    """
    RQueue class.

    An implementation of a reliable queue for Redis, see https://redis.io/commands/rpoplpush.
    """

    def __init__(self, _name, _redis, _ttl=None):
        """
        :param _name: The name of the queue.
        :param _redis: An Redis instance.
        :param _ttl: Optional TTL.
        """
        self.name = _name
        self.redis = _redis
        self.ttl = _ttl

    def __len__(self):
        return int(self.redis.llen(PATTERN.format('access', self.name))) + \
            int(self.redis.llen(PATTERN.format('process', self.name)))

    def push(self, _value, _pipe=None):
        """
        Push a value onto the back of the queue.

        :param _value: The value to put into the queue.
        :param _pipe: A Redis pipe, optional.
        :return: The amount of values in the list.
        """
        r = _pipe if _pipe else self.redis
        pushed = r.lpush(PATTERN.format('access', self.name), _value)
        if self.ttl:
            r.expire(PATTERN.format('access', self.name), self.ttl)

        return pushed

    def pop(self, _pipe=None):
        """
        Pop the next value from the top of the queue. N.B: This is a non-blocking operation.

        :param _pipe: A Redis pipe, optional.
        :return: The next value in the queue, else None.
        """
        r = _pipe if _pipe else self.redis

        return r.rpoplpush(PATTERN.format('access', self.name), PATTERN.format('process', self.name))

    def bpop(self, _to=0, _pipe=None):
        """
        Pop the next value from the top of the queue. N.B: This is a blocking operation iff the queue is empty.

        :param _to: Blocking timeout in seconds. N.B: defaults to 0, i.e. infinite
        :param _pipe: A Redis pipe, optional.
        :return: The next value in the queue, else None.
        """
        r = _pipe if _pipe else self.redis

        return r.brpoplpush(PATTERN.format('access', self.name), PATTERN.format('process', self.name), _to)

    def ack(self, _value, _pipe=None):
        """
        Acknowledge a value from the queue, i.e. successfully processed.

        :param _value: The value to be acknowledged.
        :param _pipe: A Redis pipe, optional.
        :return: Success.
        """
        r = _pipe if _pipe else self.redis

        return bool(r.lrem(PATTERN.format('process', self.name), 1, _value))
