<?php

namespace Illuminate\Redis\Connections;

use Closure;
use Illuminate\Contracts\Redis\Connection as ConnectionContract;
use RedisException;

/**
 * @mixin \Redis
 * @method string append(string $key, string $value)
 * @method bool auth(string $password)
 * @method bool bitop(string $operation, string $destKey, array $keys)
 * @method int bitpos(string $key, int $bit, int $start = 0, int $end = -1)
 * @method int bitcount(string $key)
 * @method bool blpop(array $keys, int $timeout)
 * @method bool brpop(array $keys, int $timeout)
 * @method string brpoplpush(string $source, string $destination, int $timeout)
 * @method string command()
 * @method string commandInfo(array $commands)
 * @method string commandStats()
 * @method array config(string $subcommand, ...$parameters)
 * @method array decrby(string $key, int $value)
 * @method bool del(string $key)
 * @method string decr(string $key)
 * @method array eval(string $script, array $keys, array $args)
 * @method string evalsha(string $sha1, array $keys, array $args)
 * @method string exists(string $key)
 * @method array geodist(string $key, string $member1, string $member2, string $unit = 'm')
 * @method array geohash(string $key, array $members)
 * @method array geopos(string $key, array $members)
 * @method array georadius(string $key, float $longitude, float $latitude, float $radius, string $unit = 'm', array $options = [])
 * @method array georadiusbymember(string $key, string $member, float $radius, string $unit = 'm', array $options = [])
 * @method bool geoadd(string $key, float $longitude, float $latitude, string $member)
 * @method int geoadd(string $key, float $longitude, float $latitude, string $member)
 * @method array getbit(string $key, int $offset)
 * @method string getset(string $key, string $value)
 * @method string get(string $key)
 * @method string hdel(string $key, string $field)
 * @method bool hexists(string $key, string $field)
 * @method array hgetall(string $key)
 * @method array hgetall(string $key)
 * @method string hget(string $key, string $field)
 * @method int hincrby(string $key, string $field, int $value)
 * @method bool hscan(string $key, int $cursor, string $pattern = '', int $count = 10)
 * @method bool hset(string $key, string $field, string $value)
 * @method bool hsetnx(string $key, string $field, string $value)
 * @method bool hmget(string $key, array $fields)
 * @method bool hmset(string $key, array $fields)
 * @method bool hvals(string $key)
 * @method int hlen(string $key)
 * @method int incr(string $key)
 * @method string incrby(string $key, int $value)
 * @method string info()
 * @method string lastsave()
 * @method bool lpop(string $key)
 * @method bool lpush(string $key, string $value)
 * @method bool lpushx(string $key, string $value)
 * @method int llen(string $key)
 * @method array lrange(string $key, int $start, int $stop)
 * @method bool lrem(string $key, int $count, string $value)
 * @method bool lset(string $key, int $index, string $value)
 * @method string ltrim(string $key, int $start, int $stop)
 * @method string move(string $key, int $db)
 * @method bool mset(array $keyValuePairs)
 * @method bool msetnx(array $keyValuePairs)
 * @method array mget(array $keys)
 * @method string ping()
 * @method bool publish(string $channel, string $message)
 * @method string randomkey()
 * @method bool renameNx(string $oldKey, string $newKey)
 * @method string rename(string $oldKey, string $newKey)
 * @method bool restore(string $key, int $ttl, string $serializedValue)
 * @method bool restoreReplace(string $key, int $ttl, string $serializedValue)
 * @method bool rpop(string $key)
 * @method bool rpush(string $key, string $value)
 * @method bool rpushx(string $key, string $value)
 * @method array sdiff(string $key1, string $key2)
 * @method string sadd(string $key, string $member)
 * @method string sinter(string $key1, string $key2)
 * @method array smembers(string $key)
 * @method bool smove(string $source, string $destination, string $member)
 * @method bool sismember(string $key, string $member)
 * @method array srandmember(string $key, int $count)
 * @method string spop(string $key)
 * @method string sunion(string $key1, string $key2)
 * @method bool zrem(string $key, string $member)
 * @method array zrange(string $key, int $start, int $stop)
 * @method array zrevrange(string $key, int $start, int $stop)
 * @method bool zscan(string $key, int $cursor, string $pattern = '', int $count = 10)
 * @method bool zadd(string $key, float $score, string $member)
 * @method bool zincrby(string $key, float $increment, string $member)
 * @method int zcard(string $key)
 * @method array zcount(string $key, float $min, float $max)
 * @method int zrank(string $key, string $member)
 * @method array zrangebyscore(string $key, float $min, float $max)
 * @method array zrevrangebyscore(string $key, float $max, float $min)
 * @method string zscore(string $key, string $member)
 * @method int zremrangebyscore(string $key, float $min, float $max)
 * @method int zremrangebyrank(string $key, int $start, int $stop)
 * @method string zunionstore(string $destination, array $sets, array $weights = [], string $aggregate = 'sum')
 * @method bool zinterstore(string $destination, array $sets, array $weights = [], string $aggregate = 'sum')
 * @method bool setbit(string $key, int $offset, bool $value)
 * @method string setex(string $key, int $seconds, string $value)
 * @method bool setnx(string $key, string $value)
 * @method bool set(string $key, string $value)
 * @method bool touch(string $key)
 * @method bool ttl(string $key)
 * @method string type(string $key)
 * @method bool unlink(string $key)
 * @method bool xack(string $key, string $group, array $ids)
 * @method array xrange(string $key, string $start, string $end, int $count = 10)
 * @method string xdel(string $key, array $ids)
 * @method array xread(array $streams, string $count = '1', string $block = '0')
 * @method array xreadgroup(string $group, string $consumer, array $streams, string $count = '1', string $block = '0')
 * @method string xtrim(string $key, int $maxLength)
 * @method int xlen(string $key)
 * @method bool zscan(string $key, int $cursor, string $pattern = '', int $count = 10)
 * @method string zadd(string $key, float $score, string $member)
 * @method string zincrby(string $key, float $increment, string $member)
 * @method array zrangebyscore(string $key, float $min, float $max)
 * @method array zrevrangebyscore(string $key, float $max, float $min)
 * @method string zscore(string $key, string $member)
 * @method string zinterstore(string $destination, array $keys, array $weights = [], string $aggregate = 'sum')
 * @method string zunionstore(string $destination, array $keys, array $weights = [], string $aggregate = 'sum')
 * @method string zremrangebyscore(string $key, float $min, float $max)
 * @method bool zremrangebyrank(string $key, int $start, int $stop)
 * @method int zcount(string $key, float $min, float $max)
 * @method bool zscan(string $key, int $cursor, string $pattern = '', int $count = 10)
 */
class PhpRedisConnection extends Connection implements ConnectionContract
{
    use PacksPhpRedisValues;

    /**
     * The connection creation callback.
     *
     * @var callable
     */
    protected $connector;

    /**
     * The connection configuration array.
     *
     * @var array
     */
    protected $config;

    /**
     * Create a new PhpRedis connection.
     *
     * @param  \Redis  $client
     * @param  callable|null  $connector
     * @param  array  $config
     * @return void
     */
    public function __construct($client, ?callable $connector = null, array $config = [])
    {
        $this->client = $client;
        $this->config = $config;
        $this->connector = $connector;
    }

    /**
     * Returns the value of the given key.
     *
     * @param  string  $key
     * @return string|null
     */
    public function get($key)
    {
        $result = $this->command('get', [$key]);

        return $result !== false ? $result : null;
    }

    /**
     * Get the values of all the given keys.
     *
     * @param  array  $keys
     * @return array
     */
    public function mget(array $keys)
    {
        return array_map(function ($value) {
            return $value !== false ? $value : null;
        }, $this->command('mget', [$keys]));
    }

    /**
     * Set the string value in the argument as the value of the key.
     *
     * @param  string  $key
     * @param  mixed  $value
     * @param  string|null  $expireResolution
     * @param  int|null  $expireTTL
     * @param  string|null  $flag
     * @return bool
     */
    public function set($key, $value, $expireResolution = null, $expireTTL = null, $flag = null)
    {
        return $this->command('set', [
            $key,
            $value,
            $expireResolution ? [$flag, $expireResolution => $expireTTL] : null,
        ]);
    }

    /**
     * Set the given key if it doesn't exist.
     *
     * @param  string  $key
     * @param  string  $value
     * @return int
     */
    public function setnx($key, $value)
    {
        return (int) $this->command('setnx', [$key, $value]);
    }

    /**
     * Get the value of the given hash fields.
     *
     * @param  string  $key
     * @param  mixed  ...$dictionary
     * @return array
     */
    public function hmget($key, ...$dictionary)
    {
        if (count($dictionary) === 1) {
            $dictionary = $dictionary[0];
        }

        return array_values($this->command('hmget', [$key, $dictionary]));
    }

    /**
     * Set the given hash fields to their respective values.
     *
     * @param  string  $key
     * @param  mixed  ...$dictionary
     * @return int
     */
    public function hmset($key, ...$dictionary)
    {
        if (count($dictionary) === 1) {
            $dictionary = $dictionary[0];
        } else {
            $input = collect($dictionary);

            $dictionary = $input->nth(2)->combine($input->nth(2, 1))->toArray();
        }

        return $this->command('hmset', [$key, $dictionary]);
    }

    /**
     * Set the given hash field if it doesn't exist.
     *
     * @param  string  $hash
     * @param  string  $key
     * @param  string  $value
     * @return int
     */
    public function hsetnx($hash, $key, $value)
    {
        return (int) $this->command('hsetnx', [$hash, $key, $value]);
    }

    /**
     * Removes the first count occurrences of the value element from the list.
     *
     * @param  string  $key
     * @param  int  $count
     * @param  mixed  $value
     * @return int|false
     */
    public function lrem($key, $count, $value)
    {
        return $this->command('lrem', [$key, $value, $count]);
    }

    /**
     * Removes and returns the first element of the list stored at key.
     *
     * @param  mixed  ...$arguments
     * @return array|null
     */
    public function blpop(...$arguments)
    {
        $result = $this->command('blpop', $arguments);

        return empty($result) ? null : $result;
    }

    /**
     * Removes and returns the last element of the list stored at key.
     *
     * @param  mixed  ...$arguments
     * @return array|null
     */
    public function brpop(...$arguments)
    {
        $result = $this->command('brpop', $arguments);

        return empty($result) ? null : $result;
    }

    /**
     * Removes and returns a random element from the set value at key.
     *
     * @param  string  $key
     * @param  int|null  $count
     * @return mixed|false
     */
    public function spop($key, $count = 1)
    {
        return $this->command('spop', func_get_args());
    }

    /**
     * Add one or more members to a sorted set or update its score if it already exists.
     *
     * @param  string  $key
     * @param  mixed  ...$dictionary
     * @return int
     */
    public function zadd($key, ...$dictionary)
    {
        if (is_array(end($dictionary))) {
            foreach (array_pop($dictionary) as $member => $score) {
                $dictionary[] = $score;
                $dictionary[] = $member;
            }
        }

        $options = [];

        foreach (array_slice($dictionary, 0, 3) as $i => $value) {
            if (in_array($value, ['nx', 'xx', 'ch', 'incr', 'gt', 'lt', 'NX', 'XX', 'CH', 'INCR', 'GT', 'LT'], true)) {
                $options[] = $value;

                unset($dictionary[$i]);
            }
        }

        return $this->command('zadd', array_merge([$key], [$options], array_values($dictionary)));
    }

    /**
     * Return elements with score between $min and $max.
     *
     * @param  string  $key
     * @param  mixed  $min
     * @param  mixed  $max
     * @param  array  $options
     * @return array
     */
    public function zrangebyscore($key, $min, $max, $options = [])
    {
        if (isset($options['limit']) && ! array_is_list($options['limit'])) {
            $options['limit'] = [
                $options['limit']['offset'],
                $options['limit']['count'],
            ];
        }

        return $this->command('zRangeByScore', [$key, $min, $max, $options]);
    }

    /**
     * Return elements with score between $min and $max.
     *
     * @param  string  $key
     * @param  mixed  $min
     * @param  mixed  $max
     * @param  array  $options
     * @return array
     */
    public function zrevrangebyscore($key, $min, $max, $options = [])
    {
        if (isset($options['limit']) && ! array_is_list($options['limit'])) {
            $options['limit'] = [
                $options['limit']['offset'],
                $options['limit']['count'],
            ];
        }

        return $this->command('zRevRangeByScore', [$key, $min, $max, $options]);
    }

    /**
     * Find the intersection between sets and store in a new set.
     *
     * @param  string  $output
     * @param  array  $keys
     * @param  array  $options
     * @return int
     */
    public function zinterstore($output, $keys, $options = [])
    {
        return $this->command('zinterstore', [$output, $keys,
            $options['weights'] ?? null,
            $options['aggregate'] ?? 'sum',
        ]);
    }

    /**
     * Find the union between sets and store in a new set.
     *
     * @param  string  $output
     * @param  array  $keys
     * @param  array  $options
     * @return int
     */
    public function zunionstore($output, $keys, $options = [])
    {
        return $this->command('zunionstore', [$output, $keys,
            $options['weights'] ?? null,
            $options['aggregate'] ?? 'sum',
        ]);
    }

    /**
     * Scans all keys based on options.
     *
     * @param  mixed  $cursor
     * @param  array  $options
     * @return mixed
     */
    public function scan($cursor, $options = [])
    {
        $result = $this->client->scan($cursor,
            $options['match'] ?? '*',
            $options['count'] ?? 10
        );

        if ($result === false) {
            $result = [];
        }

        return $cursor === 0 && empty($result) ? false : [$cursor, $result];
    }

    /**
     * Scans the given set for all values based on options.
     *
     * @param  string  $key
     * @param  mixed  $cursor
     * @param  array  $options
     * @return mixed
     */
    public function zscan($key, $cursor, $options = [])
    {
        $result = $this->client->zscan($key, $cursor,
            $options['match'] ?? '*',
            $options['count'] ?? 10
        );

        if ($result === false) {
            $result = [];
        }

        return $cursor === 0 && empty($result) ? false : [$cursor, $result];
    }

    /**
     * Scans the given hash for all values based on options.
     *
     * @param  string  $key
     * @param  mixed  $cursor
     * @param  array  $options
     * @return mixed
     */
    public function hscan($key, $cursor, $options = [])
    {
        $result = $this->client->hscan($key, $cursor,
            $options['match'] ?? '*',
            $options['count'] ?? 10
        );

        if ($result === false) {
            $result = [];
        }

        return $cursor === 0 && empty($result) ? false : [$cursor, $result];
    }

    /**
     * Scans the given set for all values based on options.
     *
     * @param  string  $key
     * @param  mixed  $cursor
     * @param  array  $options
     * @return mixed
     */
    public function sscan($key, $cursor, $options = [])
    {
        $result = $this->client->sscan($key, $cursor,
            $options['match'] ?? '*',
            $options['count'] ?? 10
        );

        if ($result === false) {
            $result = [];
        }

        return $cursor === 0 && empty($result) ? false : [$cursor, $result];
    }

    /**
     * Execute commands in a pipeline.
     *
     * @param  callable|null  $callback
     * @return \Redis|array
     */
    public function pipeline(?callable $callback = null)
    {
        $pipeline = $this->client()->pipeline();

        return is_null($callback)
            ? $pipeline
            : tap($pipeline, $callback)->exec();
    }

    /**
     * Execute commands in a transaction.
     *
     * @param  callable|null  $callback
     * @return \Redis|array
     */
    public function transaction(?callable $callback = null)
    {
        $transaction = $this->client()->multi();

        return is_null($callback)
            ? $transaction
            : tap($transaction, $callback)->exec();
    }

    /**
     * Evaluate a LUA script serverside, from the SHA1 hash of the script instead of the script itself.
     *
     * @param  string  $script
     * @param  int  $numkeys
     * @param  mixed  ...$arguments
     * @return mixed
     */
    public function evalsha($script, $numkeys, ...$arguments)
    {
        return $this->command('evalsha', [
            $this->script('load', $script), $arguments, $numkeys,
        ]);
    }

    /**
     * Evaluate a script and return its result.
     *
     * @param  string  $script
     * @param  int  $numberOfKeys
     * @param  mixed  ...$arguments
     * @return mixed
     */
    public function eval($script, $numberOfKeys, ...$arguments)
    {
        return $this->command('eval', [$script, $arguments, $numberOfKeys]);
    }

    /**
     * Subscribe to a set of given channels for messages.
     *
     * @param  array|string  $channels
     * @param  \Closure  $callback
     * @return void
     */
    public function subscribe($channels, Closure $callback)
    {
        $this->client->subscribe((array) $channels, function ($redis, $channel, $message) use ($callback) {
            $callback($message, $channel);
        });
    }

    /**
     * Subscribe to a set of given channels with wildcards.
     *
     * @param  array|string  $channels
     * @param  \Closure  $callback
     * @return void
     */
    public function psubscribe($channels, Closure $callback)
    {
        $this->client->psubscribe((array) $channels, function ($redis, $pattern, $channel, $message) use ($callback) {
            $callback($message, $channel);
        });
    }

    /**
     * Subscribe to a set of given channels for messages.
     *
     * @param  array|string  $channels
     * @param  \Closure  $callback
     * @param  string  $method
     * @return void
     */
    public function createSubscription($channels, Closure $callback, $method = 'subscribe')
    {
        //
    }

    /**
     * Flush the selected Redis database.
     *
     * @return mixed
     */
    public function flushdb()
    {
        $arguments = func_get_args();

        if (strtoupper((string) ($arguments[0] ?? null)) === 'ASYNC') {
            return $this->command('flushdb', [true]);
        }

        return $this->command('flushdb');
    }

    /**
     * Execute a raw command.
     *
     * @param  array  $parameters
     * @return mixed
     */
    public function executeRaw(array $parameters)
    {
        return $this->command('rawCommand', $parameters);
    }

    /**
     * Run a command against the Redis database.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     *
     * @throws \RedisException
     */
    public function command($method, array $parameters = [])
    {
        try {
            return parent::command($method, $parameters);
        } catch (RedisException $e) {
            foreach (['went away', 'socket', 'read error on connection', 'Connection lost'] as $errorMessage) {
                if (str_contains($e->getMessage(), $errorMessage)) {
                    $this->client = $this->connector ? call_user_func($this->connector) : $this->client;

                    break;
                }
            }

            throw $e;
        }
    }

    /**
     * Disconnects from the Redis instance.
     *
     * @return void
     */
    public function disconnect()
    {
        $this->client->close();
    }

    /**
     * Pass other method calls down to the underlying client.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return parent::__call(strtolower($method), $parameters);
    }
}
