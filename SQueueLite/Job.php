<?php

namespace Schorsch3000\SQueueLite;

use function unserialize;

class Job
{
    private $job_id;
    private $queue_name;
    private $job_input;
    private $db;

    /**
     * @param $job_id
     * @param $queue_name
     * @param $job_input
     * @param $db
     */
    public function __construct($job_id, $queue_name, $job_input, $db)
    {
        $this->job_id = $job_id;
        $this->queue_name = $queue_name;
        $this->job_input = unserialize($job_input);
        $this->db = $db;
    }
    public static function CreateFromDBRow($row, $db)
    {
        return new Job(
            $row["job_id"],
            $row["queue_name"],
            $row["job_input"],
            $db
        );
    }
    public function getJobId()
    {
        return $this->job_id;
    }
    public function getQueueName()
    {
        return $this->queue_name;
    }
    public function getJobInput()
    {
        return $this->job_input;
    }
    public function reportWorking()
    {
        $stmt = $this->db->prepare(
            "UPDATE queue SET locked_timestamp = :locked_timestamp WHERE job_id = :job_id"
        );
        $this->db->beginTransaction();
        $stmt->execute([
            "locked_timestamp" => time(),
            "job_id" => $this->job_id,
        ]);
        $this->db->commit();
    }
    public function reportDone($result)
    {
        $stmt = $this->db->prepare(
            "UPDATE queue SET job_output = :job_output, locked_timestamp = :locked_timestamp, job_status = 2 WHERE job_id = :job_id"
        );
        $this->db->beginTransaction();
        $stmt->execute([
            "job_output" => serialize($result),
            "locked_timestamp" => null,
            "job_id" => $this->job_id,
        ]);
        $this->db->commit();
    }
}
