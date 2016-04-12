<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\queue;


use Yii;
use yii\tarantool\Connection;
use yii\base\Component;
use yii\base\InvalidConfigException;

/**
 * TarantoolQueue
 *
 */
class TarantoolQueue extends Component implements QueueInterface
{

    const STATUS_READY = 'r';
    const STATUS_TAKEN = 't';  //the task has been taken by a consumer
    const STATUS_DONE = '-'; //the task is executed (a task is pruned from the queue when it's executed, so this state may be hard to see)
    const STATUS_BURIED = '!'; // the task is buried (disabled temporarily until further changes)
    const STATUS_DELAYED = '~';  //the task is delayed for some time

    /**
     * @var Connection|array
     */
    public $tarantool;


    public $timeout = 2;

    public $tubeType = 'fifottl';
    public $queue;

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        if ($this->tarantool === null) {
            throw new InvalidConfigException('The "tarantool" property must be set.');
        }
        if (!$this->tarantool instanceof Connection) {
            $this->addClient();
        }
    }

    private function addClient()
    {
        $client          = new Connection($this->tarantool);
        $this->tarantool = $client->tarantoolClient;
    }

    /**
     * @inheritdoc
     */
    public function push($payload, $queue = false, $delay = 0)
    {
        $queue = $queue ? $queue : $this->queue;
        $args  = ($delay != 0) ? ['delay' => $delay] : [];
        return $this->put($payload, $queue, $args);
    }

    /**
     * @inheritdoc
     */
    public function pop($queue = false)
    {
        $queue    = $queue ? $queue : $this->queue;
        $response = $this->tarantool->tarantoolClient->call("queue.tube." . $queue . ':take', $this->timeout);
        return $response;
    }


    /**
     * @inheritdoc
     */
    public function purge($queue)
    {
        $queue = $queue ? $queue : $this->queue;
        $this->tarantool->tarantoolClient->call("queue.tube." . $queue . ':drop');
        $this->tarantool->tarantoolClient->evaluate('create_tube(...)', [$queue, $this->tubeType]);
    }

    /**
     * @inheritdoc
     */
    public function release(array $message, $delay = 0)
    {
        $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':release', [
            $message[0],
            ['delay' => $delay]
        ]);
    }

    /**
     * @inheritdoc
     */
    public function delete(array $message)
    {
        $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':delete', $message[0]);
    }


    public function put($payload, $queue, $args = false)
    {
        $params = [$payload];
        if ($args) {
            $params[] = $args;
        }
        $queue = $queue ? $queue : $this->queue;
        return $this->tarantool->tarantoolClient->call("queue.tube." . $queue . ':put', $params);
    }

    public function peek($queue, $id)
    {
        $queue = $queue ? $queue : $this->queue;
        return $this->tarantool->tarantoolClient->call("queue.tube." . $queue . ':peek', [$id]);
    }

    public function ack($id)
    {
        return $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':ack', [$id]);
    }

    public function bury($id)
    {
        return $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':bury', [$id]);
    }

    public function kick($count)
    {
        return $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':kick', $count);
    }

}
