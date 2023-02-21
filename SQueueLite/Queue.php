<?php

namespace Schorsch3000\SQueueLite;

use PDO;

class Queue
{
    private $db;
    private $stallTimeout = 300;
    private $maxRetries = 3;
    private $keepFailedJobsTimeout = 3600;

    private $keepReadyJobsTimeout = 3600;

    public function __construct($dbFilePath)
    {
        $this->db = new PDO("sqlite:$dbFilePath");
        $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->db->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        $this->db->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
        $this->db->exec("PRAGMA journal_mode=TRUNCATE;");
        $this->db->exec("PRAGMA synchronous=OFF;");
        $this->db->exec(file_get_contents(__DIR__ . "/../queue.sql"));
    }

    public function getQueues()
    {
        $stmt = $this->db->prepare(
            "SELECT queue_name, COUNT(*) as count FROM queue WHERE job_status = 0 GROUP BY queue_name"
        );
        $stmt->execute();
        $result = [];
        foreach ($stmt->fetchAll() as $row) {
            $result[$row["queue_name"]] = $row["count"];
        }

        return $result;
    }
    public function getNextJob($queueList = false)
    {
        $this->cleanup();
        $data = [];
        $useFilter = false !== $queueList;
        if ($useFilter) {
            if (is_scalar($queueList)) {
                $queueList = [$queueList];
            }
            $filter = " AND (queue_name IN (";
            $i = 0;
            foreach ($queueList as $queueName) {
                $data["queueName" . $i] = $queueName;
                $filter .= ":queueName" . $i++ . ", ";
            }
            $filter = substr($filter, 0, -2);
            $filter .= "))";
        } else {
            $filter = "";
        }

        $stmt = $this->db->prepare(
            "SELECT * FROM queue WHERE job_status = 0  $filter  ORDER BY created_timestamp ASC LIMIT 1"
        );
        $this->db->beginTransaction();
        $stmt->execute($data);
        $job = $stmt->fetch();
        if (!$job) {
            $this->db->commit();
            return false;
        }
        $stmt = $this->db->prepare(
            "UPDATE queue SET locked_timestamp=:locked_timestamp, job_status = 1 WHERE job_id=:job_id"
        );
        $stmt->execute([
            "locked_timestamp" => time(),
            "job_id" => $job["job_id"],
        ]);

        $this->db->commit();
        return Job::CreateFromDBRow($job, $this->db);
    }
    public function addJob($queueName, $jobData)
    {
        $this->db->beginTransaction();
        $stmt = $this->db->prepare(
            "INSERT INTO queue (queue_name, job_input, created_timestamp) VALUES (:queue_name, :job_data, :created_at)"
        );
        $stmt->execute([
            "queue_name" => $queueName,
            "job_data" => serialize($jobData),
            "created_at" => time(),
        ]);
        $this->db->commit();
        return $this->db->lastInsertId();
    }

    public function deleteJob($jobId)
    {
        $stmt = $this->db->prepare("DELETE FROM queue WHERE job_id=:job_id");
        $stmt->execute([
            "job_id" => $jobId,
        ]);
    }
    public function cleanup()
    {
        $this->db->beginTransaction();
        $stmt = $this->db->prepare(
            "DELETE FROM queue WHERE job_status = 2 AND created_timestamp < :created_timestamp"
        );
        $stmt->execute([
            "created_timestamp" => time() - $this->keepReadyJobsTimeout,
        ]);

        $stmt = $this->db->prepare(
            "DELETE FROM queue WHERE job_status = 3 AND created_timestamp < :created_timestamp"
        );
        $stmt->execute([
            "created_timestamp" => time() - $this->keepFailedJobsTimeout,
        ]);

        $stmt = $this->db->prepare(
            "UPDATE queue set job_status = 0 , locked_timestamp = NULL, reset_count = reset_count+1 WHERE job_status = 1 AND created_timestamp < :created_timestamp"
        );
        $stmt->execute([
            "created_timestamp" => time() - $this->stallTimeout,
        ]);
        $stmt= $this->db->prepare(
            "UPDATE queue set job_status = 3 WHERE reset_count >= :max_retries"
        );
        $stmt->execute([
            "max_retries" => $this->maxRetries,
        ]);
        $this->db->commit();
    }

    /**
     * @return int
     */
    public function getStallTimeout(): int
    {
        return $this->stallTimeout;
    }

    /**
     * @param int $stallTimeout
     * @return Queue
     */
    public function setStallTimeout(int $stallTimeout): Queue
    {
        if ($stallTimeout < 1) {
            throw new \InvalidArgumentException("StallTimeout must be >= 1");
        }
        $this->stallTimeout = $stallTimeout;
        return $this;
    }

    /**
     * @return int
     */
    public function getMaxRetries(): int
    {
        return $this->maxRetries;
    }

    /**
     * @param int $maxRetries
     * @return Queue
     */
    public function setMaxRetries(int $maxRetries): Queue
    {
        if ($maxRetries < 0) {
            throw new \InvalidArgumentException("MaxRetries must be >= 0");
        }
        $this->maxRetries = $maxRetries;
        return $this;
    }

    /**
     * @return int
     */
    public function getKeepFailedJobsTimeout(): int
    {
        return $this->keepFailedJobsTimeout;
    }

    /**
     * @param int $keepFailedJobsTimeout
     * @return Queue
     */
    public function setKeepFailedJobsTimeout(int $keepFailedJobsTimeout): Queue
    {
        if ($keepFailedJobsTimeout < 0) {
            throw new \InvalidArgumentException(
                "KeepFailedJobsTimeout must be >= 0"
            );
        }
        $this->keepFailedJobsTimeout = $keepFailedJobsTimeout;
        return $this;
    }

    public function getIfReady($jobId)
    {
        $this->cleanup();
        $stmt = $this->db->prepare(
            "SELECT job_output FROM queue WHERE job_id=:job_id AND job_status = 2"
        );
        $stmt->execute([
            "job_id" => $jobId,
        ]);
        $column = $stmt->fetchColumn();

        if (false === $column) {
            return false;
        }
        return unserialize($column);
    }

    /**
     * @return int
     */
    public function getKeepReadyJobsTimeout(): int
    {
        return $this->keepReadyJobsTimeout;
    }

    /**
     * @param int $keepReadyJobsTimeout
     * @return Queue
     */
    public function setKeepReadyJobsTimeout(int $keepReadyJobsTimeout): Queue
    {
        $this->keepReadyJobsTimeout = $keepReadyJobsTimeout;
        return $this;
    }
}
