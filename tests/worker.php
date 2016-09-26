<?php
require_once 'bootstrapWorker.php';

use yii\queue\TarantoolQueue;
use yii\tarantool\Connection;

define('QUEUE', 32112345);

$tube = 'q1';

function getConnection()
{
    $tarantoolConfig = [
        'host'     => 'localhost',
        'port'     => '3301',
        'username' => '',
        'password' => '',
        'options'  => [],
    ];

    $connection       = new Connection();
    $connection->host = $tarantoolConfig['host'];
    $connection->port = $tarantoolConfig['port'];
    if(isset($tarantoolConfig['username']))
    {
        $connection->username = $tarantoolConfig['username'];
    }
    if(isset($tarantoolConfig['password']))
    {
        $connection->password = $tarantoolConfig['password'];
    }
    $connection->open();

    return $connection;
}

$tarantool = getConnection();
$tarantoolQueue     = new TarantoolQueue(
    [
        'tarantool' => $tarantool,
        'queue'     => $tube,
        'tubeType'  => TarantoolQueue::TYPE_FIFOTTL,
        'timeout'   => 1
    ]
);

$queue = msg_get_queue(QUEUE);

$msg_type     = null;
$msg          = null;
$max_msg_size = 512;

while (msg_receive($queue, 1, $msg_type, $max_msg_size, $msg))
{
    $actualResult = $tarantoolQueue->push($msg, $tube);
    echo 'Message pulled from queue - msg id: ' . $msg->id . ', msg name: ' . $msg->name . PHP_EOL;
    $msg_type = null;
    $msg      = null;
}