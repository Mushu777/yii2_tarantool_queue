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
    /**
     * status when task ready
     */
    const STATUS_READY = 'r';

    /**
     * status when task has been taken by a consumer
     */
    const STATUS_TAKEN = 't';

    /**
     * status when task executed
     * (a task is pruned from the queue when it's executed, so this state may be hard to see)
     */
    const STATUS_DONE = '-';

    /**
     * status when task buried
     * (disabled temporarily until further changes)
     */
    const STATUS_BURIED = '!';

    /**
     * status when task delayed for some time
     */
    const STATUS_DELAYED = '~';

    /**
     * simple queue
     */
    const TYPE_FIFO = 'fifo';

    /**
     * simple priority queue with task time to live support
     */
    const TYPE_FIFOTTL = 'fifottl';

    /**
     * queue with micro-queues inside
     */
    const TYPE_UTUBE = 'utube';

    /**
     * extention of utube to support ttl
     */
    const TYPE_UTUBETTL = 'utubettl';

    /**
     * @var Connection|array Tarantool conection object or config for connection
     */
    protected $tarantool;

    /**
     * @var int timeout value
     */
    public $timeout = 2;

    /**
     * @var string tube name
     */
    public $queue;

    /**
     * @var string tube type
     */
    public $tubeType = self::TYPE_FIFOTTL;

    /**
     * initialization
     */
    public function init()
    {
        parent::init();
        if ($this->tarantool === null) {
            throw new InvalidConfigException('The "tarantool" property must be set.');
        }
        if (!$this->tarantool instanceof Connection) {
            $client          = new Connection($this->tarantool);
            $this->tarantool = $client->tarantoolClient;
            $this->connectionOpen();
        }
    }

    /**
     * Установление соединения
     * @throws InvalidConfigException
     */
    public function connectionOpen()
    {
        if (!$this->tarantool instanceof Connection) {
            throw new InvalidConfigException('The "tarantool" property must be set.');
        }
        $this->tarantool->open();
    }

    /**
     * Закрытие соединения
     * @throws InvalidConfigException
     */
    public function connectionClose()
    {
        if (!$this->tarantool instanceof Connection) {
            throw new InvalidConfigException('The "tarantool" property must be set.');
        }
        $this->tarantool->close();
    }

    /**
     * @param Connection $tarantool
     * @throws InvalidConfigException
     */
    public function setTarantool($tarantool)
    {
        if ($this->tarantool instanceof Connection) {
            throw new InvalidConfigException('The "tarantool" property already set.');
        }
        $this->tarantool = $tarantool;
    }

    /**
     * кладет задачу в очередь
     * @param mixed $payload Задача
     * @param bool|string $queue название очереди
     * @param int $delay задержка
     * @return mixed
     */
    public function push($payload, $queue = false, $delay = 0)
    {
        $queue = $queue ? $queue : $this->queue;
        $args  = ($delay != 0) ? ['delay' => $delay] : [];
        return $this->put($payload, $queue, $args);
    }

    /**
     * берет задачу из очереди
     * @param bool|string $queue название очереди
     * @return mixed задача
     */
    public function pop($queue = false)
    {
        $queue    = $queue ? $queue : $this->queue;
        $response = $this->tarantool->tarantoolClient->call("queue.tube." . $queue . ':take', $this->timeout);
        return $response;
    }


    /**
     * Очистка очереди
     * @param bool|string $queue название очереди
     */
    public function purge($queue)
    {
        $queue = $queue ? $queue : $this->queue;
        $this->tarantool->tarantoolClient->call("queue.tube." . $queue . ':drop');
        $this->tarantool->tarantoolClient->evaluate('create_tube(...)', [$queue, $this->tubeType]);
    }

    /**
     * Возвращение задачи в очереди на статус ready
     * @param array $task
     * @param int $delay
     */
    public function release(array $task, $delay = 0)
    {
        $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':release', [
            $task[0],
            ['delay' => $delay]
        ]);
    }

    /**
     * Удаление задачи из очереди
     * @param array $message
     */
    public function delete(array $task)
    {
        $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':delete', $task[0]);
    }

    /**
     * кладет задачу в очередь
     * @param mixed $payload
     * @param string $queue
     * @param bool $args
     * @return mixed
     */
    public function put($payload, $queue, $args = false)
    {
        $params = [$payload];
        if ($args) {
            $params[] = $args;
        }
        $queue = $queue ? $queue : $this->queue;
        return $this->tarantool->tarantoolClient->call("queue.tube." . $queue . ':put', $params);
    }

    /**
     * возвращает данные задачи из очереди не изменяя ее
     * @param string $queue
     * @param int $id
     * @return mixed
     */
    public function peek($queue, $id)
    {
        $queue = $queue ? $queue : $this->queue;
        return $this->tarantool->tarantoolClient->call("queue.tube." . $queue . ':peek', [$id]);
    }

    /**
     * Сообщает очереди об успеешном завершении задачи
     * @param int $id
     * @return mixed
     */
    public function ack($id)
    {
        return $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':ack', [$id]);
    }

    /**
     * Устанавливает задачу в состояние при котором она временно не может обрабатываться
     * @param int $id
     * @return mixed
     */
    public function bury($id)
    {
        return $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':bury', [$id]);
    }

    /**
     * Возвращает приостановленные задачи в обработку
     * @param int $count
     * @return mixed
     */
    public function kick($count)
    {
        return $this->tarantool->tarantoolClient->call("queue.tube." . $this->queue . ':kick', $count);
    }

}
