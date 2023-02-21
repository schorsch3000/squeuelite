<?php

namespace Tests\Unit;

use Schorsch3000\SQueueLite\Job;
use Schorsch3000\SQueueLite\Queue;
use function file_put_contents;
use function get_class;
use function is_file;
use function PHPUnit\Framework\assertEquals;
use function sleep;
use function tempnam;
use function var_dump;
use function var_export;

class QueueTest extends \Codeception\Test\Unit
{
    private $dbFile;
    protected function _before()
    {
        $this->dbFile = tempnam(sys_get_temp_dir(), "queue");
    }

    protected function _after()
    {
        if (is_file($this->dbFile)) {
            unlink($this->dbFile);
        }
    }

    // tests
    public function testCreatedDbFile()
    {
        $queue = new Queue($this->dbFile);
        $this->assertFileExists($this->dbFile);
    }
    function testEmptyQueueList()
    {
        $queue = new Queue($this->dbFile);
        $this->assertEmpty($queue->getQueues());
        $this->assertIsArray($queue->getQueues());
    }

    function testJobIsQueueable()
    {
        $queue = new Queue($this->dbFile);
        $queue->addJob("test", "test");
        $this->assertNotEmpty($queue->getQueues());
    }

    function testSetAndReadStallTimeout()
    {
        $queue = new Queue($this->dbFile);
        $queue->setStallTimeout(123);
        $this->assertEquals(123, $queue->getStallTimeout());
    }
    function testSetAndReadMaxRetries()
    {
        $queue = new Queue($this->dbFile);
        $queue->setMaxRetries(456);
        $this->assertEquals(456, $queue->getMaxRetries());
    }
    function testtSetAndReadKeepFailedJobsTimeout()
    {
        $queue = new Queue($this->dbFile);
        $queue->setKeepFailedJobsTimeout(789);
        $this->assertEquals(789, $queue->getKeepFailedJobsTimeout());
    }

    function testJobIsWorkable()
    {
        $queue = new Queue($this->dbFile);
        $queue->addJob("test", "testData");
        $job = $queue->getNextJob("test");
        $this->assertNotEmpty($job);
        $this->assertEquals(Job::class, get_class($job));
        $this->assertEquals("test", $job->getQueueName());
        $this->assertEquals("testData", $job->getJobInput());
        $this->assertEmpty($queue->getNextJob("test"));
        $this->assertFalse($queue->getIfReady($job->getJobId()));
        $job->reportDone("testResult");
        $this->assertEmpty($queue->getNextJob("test"));
        $this->assertEquals("testResult", $queue->getIfReady($job->getJobId()));
        $this->assertEquals("testResult", $queue->getIfReady($job->getJobId()));
        $queue->deleteJob($job->getJobId());
        $this->assertFalse($queue->getIfReady($job->getJobId()));
    }

    function testJobsAreInOrder()
    {
        $queue = new Queue($this->dbFile);
        $queue->addJob("test", "testData1");
        $queue->addJob("test", "testData2");
        $queue->addJob("test", "testData3");
        $job = $queue->getNextJob("test");
        $this->assertEquals("testData1", $job->getJobInput());
        $job = $queue->getNextJob("test");
        $this->assertEquals("testData2", $job->getJobInput());
        $job = $queue->getNextJob("test");
        $this->assertEquals("testData3", $job->getJobInput());
    }
    function testOnlyJobsForTheRightQueueAreProvided()
    {
        $queue = new Queue($this->dbFile);
        $queue->addJob("test1", "testData1");
        $queue->addJob("test1", "testData1");
        $queue->addJob("test1", "testData1");
        $queue->addJob("test2", "testData2");
        $queue->addJob("test2", "testData2");
        $queue->addJob("test2", "testData2");
        $job = $queue->getNextJob("test1");
        $this->assertEquals("testData1", $job->getJobInput());
        $job = $queue->getNextJob("test2");
        $this->assertEquals("testData2", $job->getJobInput());
        $job = $queue->getNextJob("test1");
        $this->assertEquals("testData1", $job->getJobInput());
        $job = $queue->getNextJob("test2");
        $this->assertEquals("testData2", $job->getJobInput());
        $job = $queue->getNextJob("test1");
        $this->assertEquals("testData1", $job->getJobInput());
        $job = $queue->getNextJob("test2");
        $this->assertEquals("testData2", $job->getJobInput());
    }
    function testQueueFilterWithMultipleQueues()
    {
        $queue = new Queue($this->dbFile);
        $queue->addJob("test1", "testDataA");
        $queue->addJob("test2", "testDataA");
        $queue->addJob("test3", "testDataA");
        $queue->addJob("test4", "testDataA");
        $queue->addJob("test5", "testDataB");
        $queue->addJob("test6", "testDataB");

        $this->assertEquals(
            "testDataB",
            $queue->getNextJob(["test5", "test6"])->getJobInput()
        );
        $this->assertEquals(
            "testDataB",
            $queue->getNextJob(["test5", "test6"])->getJobInput()
        );

        $this->assertEquals(
            "testDataA",
            $queue
                ->getNextJob(["test1", "test2", "test3", "test4"])
                ->getJobInput()
        );
        $this->assertEquals(
            "testDataA",
            $queue
                ->getNextJob(["test1", "test2", "test3", "test4"])
                ->getJobInput()
        );
        $this->assertEquals(
            "testDataA",
            $queue
                ->getNextJob(["test1", "test2", "test3", "test4"])
                ->getJobInput()
        );
        $this->assertEquals(
            "testDataA",
            $queue
                ->getNextJob(["test1", "test2", "test3", "test4"])
                ->getJobInput()
        );
    }

    function testQueueCounter()
    {
        $queue = new Queue($this->dbFile);
        assertEquals([], $queue->getQueues());
        $queue->addJob("test1", "testDataA");
        assertEquals(["test1" => 1], $queue->getQueues());
        $queue->addJob("test1", "testDataA");
        assertEquals(["test1" => 2], $queue->getQueues());
        $queue->addJob("test2", "testDataA");
        assertEquals(["test1" => 2, "test2" => 1], $queue->getQueues());
        $queue->addJob("test2", "testDataA");
        assertEquals(["test1" => 2, "test2" => 2], $queue->getQueues());
    }

    function testStall()
    {
        $queue = new Queue($this->dbFile);
        $queue->setStallTimeout(1);
        $queue->addJob("test1", "testDataA");
        $job = $queue->getNextJob("test1");
        $this->assertEquals("testDataA", $job->getJobInput());
        $this->assertEmpty($queue->getNextJob("test1"));
        sleep(2);
        $this->assertEquals(
            "testDataA",
            $queue->getNextJob("test1")->getJobInput()
        );
    }

    function testFail()
    {
        $queue = new Queue($this->dbFile);
        $queue->setStallTimeout(1);
        $queue->setMaxRetries(1);
        $queue->addJob("test", "testDataA");
        $this->assertNotEmpty($queue->getNextJob());
        $this->assertEmpty($queue->getNextJob());
        sleep(2);
        $this->assertfalse($queue->getNextJob());
        $this->assertEmpty($x = $queue->getNextJob());

        file_put_contents("testlog", var_export($x, true));
        $this->assertEmpty($queue->getNextJob());

        sleep(2);
        $this->assertEmpty($queue->getNextJob());
    }
}
