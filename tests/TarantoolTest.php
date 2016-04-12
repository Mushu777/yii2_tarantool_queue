<?php


namespace yiiunit\extensions\queue;

use yii\base\Exception;
use yii\queue\TarantoolQueue;
use yii\tarantool\Connection;

class TarantoolTest extends \PHPUnit_Framework_TestCase
{
    private $queue;
    private $tube = 'test_tube';

    protected function setUp()
    {
        $this->tarantool       = $this->getConnection(true, true);
        $this->queue           = new TarantoolQueue(['tarantool' => $this->tarantool]);
        $this->queue->queue    = $this->tube;
        $this->queue->tubeType = 'fifottl';
        $this->tarantool->tarantoolClient->evaluate('create_tube(...)', [$this->tube, $this->queue->tubeType]);
    }

    /**
     * @dataProvider provideTaskData
     */
    public function testPush($payload)
    {
        $returnValue = [0, TarantoolQueue::STATUS_READY, $payload];

        $actualResult = $this->queue->push($payload, $this->tube);
        $this->assertEquals($actualResult[0], $returnValue);
    }

    public function provideTaskData()
    {
        return [
            [null],
            [true],
            ['foo'],
            ["\x04\x00\xa0\x00\x00"],
            [42],
            [-42],
            [4.2],
            [['foo' => 'bar', 'baz' => ['qux' => false, -4.2]]],
        ];
    }

    public function testDelayedPush()
    {
        $payload      = 'testDelayedPush';
        $returnValue  = [0, TarantoolQueue::STATUS_DELAYED, $payload];
        $delay        = 100;
        $actualResult = $this->queue->push($payload, $this->tube, $delay);
        $this->assertEquals($actualResult[0], $returnValue);
    }

    public function testPeek()
    {
        $this->queue->push('peek_0', $this->tube);
        $returnValue = [0, TarantoolQueue::STATUS_READY, 'peek_0'];
        $result      = $this->queue->peek($this->tube, 0);
        $this->assertEquals($result[0], $returnValue);
    }

    public function testPop()
    {
        $payload   = 'take';
        $setResult = $this->queue->push($payload, $this->tube);

        $returnValue = [0, TarantoolQueue::STATUS_TAKEN, $payload];

        $actualResult = $this->queue->pop($this->tube);

        $this->assertEquals($actualResult[0], $returnValue);
        return $actualResult[0];
    }

    public function testEmptyPop()
    {
        $actualResult = $this->queue->pop($this->tube);

        $this->assertEquals($actualResult, []);
    }

    public function testTTLPush()
    {
        $payload      = 'test ttl';
        $returnValue  = [0, TarantoolQueue::STATUS_READY, $payload];
        $ttl          = '1';
        $actualResult = $this->queue->put($payload, $this->tube, ['ttl' => $ttl]);
        $this->assertEquals($actualResult[0], $returnValue);
        $this->assertEquals($actualResult, $this->queue->peek($this->tube, 0));
        return $ttl;
    }

    /**
     * @depends testTTLPush
     * @expectedException Exception
     * @expectedExceptionMessage Query error 32: Task 0 not found
     */
    public function testEndTTLPush($ttl)
    {
        sleep($ttl + 1);
        $this->queue->peek($this->tube, 0);
    }

    /**
     * @expectedException Exception
     * @expectedExceptionMessage Query error 32: Task 0 not found
     */
    public function testPurge()
    {
        $payload = 'test purge';
        $this->queue->put($payload, $this->tube);
        $returnValue = [0, TarantoolQueue::STATUS_READY, $payload];
        $this->assertEquals($this->queue->peek($this->tube, 0), [$returnValue]);

        $this->queue->purge($this->tube);
        $this->queue->peek($this->tube, 0);
    }


    /**
     * @expectedException Exception
     * @expectedExceptionMessage Query error 32: Task 0 not found
     */
    public function testDelete()
    {
        $payload = 'test delete';
        $this->queue->put($payload, $this->tube);
        $returnValue = [0, TarantoolQueue::STATUS_READY, $payload];
        $this->assertEquals($this->queue->peek($this->tube, 0), [$returnValue]);

        $this->queue->delete($returnValue);
        $this->queue->peek($this->tube, 0);
    }

    public function testRelease()
    {
        $payload = 'release';
        $this->queue->push($payload, $this->tube);
        $popResult = $this->queue->pop($this->tube);
        $this->queue->release($popResult[0]);
        $returnValue = [0, TarantoolQueue::STATUS_READY, $payload];
        $this->assertEquals($this->queue->peek($this->tube, $popResult[0][0]), [$returnValue]);
    }

    /**
     * @expectedException Exception
     * @expectedExceptionMessage Query error 32: Task 0 not found
     */
    public function testAck()
    {
        $payload     = 'ack';
        $returnValue = [0, TarantoolQueue::STATUS_DONE, $payload];

        $this->queue->push($payload, $this->tube);
        $popResult = $this->queue->pop($this->tube);
        $return    = $this->queue->ack($popResult[0][0]);
        $this->assertEquals($return[0], $returnValue);
        $this->queue->peek($this->tube, $popResult[0][0]);

    }

    public function testBury()
    {
        $payload     = 'test bury';
        $returnValue = [0, TarantoolQueue::STATUS_BURIED, $payload];

        $pushResult = $this->queue->push($payload, $this->tube);

        $return = $this->queue->bury($pushResult[0][0]);

        $this->assertEquals($return[0], $returnValue);
    }

    public function testKick()
    {
        $payload     = 'test kick';
        $returnValue = [0, TarantoolQueue::STATUS_READY, $payload];

        $this->queue->push($payload, $this->tube);
        $this->queue->push($payload, $this->tube);

        $this->queue->bury(0);

        $this->queue->kick(1);

        $return = $this->queue->peek($this->tube, 0);
        $this->assertEquals($return[0], $returnValue);
    }


    /**
     * @param  boolean $reset whether to clean up the test database
     * @param  boolean $open whether to open test database
     *
     * @return \yii\tarantool\Connection
     */
    public function getConnection($reset = false, $open = true)
    {
        if (!$reset && $this->tarantool) {
            return $this->tarantool;
        }
        $connection       = new Connection();
        $connection->host = $this->tarantoolConfig['host'];
        $connection->port = $this->tarantoolConfig['port'];
        if (isset($this->tarantoolConfig['username'])) {
            $connection->username = $this->tarantoolConfig['username'];
        }
        if (isset($this->tarantoolConfig['password'])) {
            $connection->password = $this->tarantoolConfig['password'];
        }
        if ($open) {
            $connection->open();
        }
        $this->tarantool = $connection;

        return $connection;
    }

    /**
     * @var array Tarantool connection configuration.
     */
    protected $tarantoolConfig = [
        'host'     => 'tarantool.dev.itass.local',
        'port'     => '3301',
        'username' => 'developer',
        'password' => 'developer',
        'options'  => [],
    ];
    /**
     * @var Connection Tarantool connection instance.
     */
    protected $tarantool;


    protected function tearDown()
    {
        if ($this->tarantool) {
            $this->tarantool->close();
        }
    }

}
