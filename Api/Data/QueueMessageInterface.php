<?php

namespace Rcason\MqMysql\Api\Data;

interface QueueMessageInterface
{
    const STATUS_TO_PROCESS = 0;
    const STATUS_MAX_RETRIES_EXCEEDED = 1;
    
    const ENTITY_ID = 'entity_id';
    const CREATED_AT = 'created_at';
    const UPDATED_AT = 'updated_at';
    const STATUS = 'status';
    const RETRIES = 'retries';
    const QUEUE_NAME = 'queue_name';
    const MESSAGE_CONTENT = 'message_content';
    const RUN_TASK_AT = 'run_task_at';
    /**
     * Claim token of the consumer currently holding the message (`owner:nonce`), null when unclaimed.
     * A message with a token whose lease (LOCKED_UNTIL) has passed is runnable again.
     */
    const LOCKED_BY = 'locked_by';
    /** UTC datetime the current claim expires. */
    const LOCKED_UNTIL = 'locked_until';
    
    /**
     * @return int
     */
    public function getEntityId();
    
    /**
     * @param int $entityId
     * @return $this
     */
    public function setEntityId($entityId);
    
    /**
     * @return string
     */
    public function getCreatedAt();
    
    /**
     * @param string $timestamp
     * @return $this
     */
    public function setCreatedAt($timestamp);
    
    /**
     * @return string
     */
    public function getUpdatedAt();
    
    /**
     * @param string $timestamp
     * @return $this
     */
    public function setUpdatedAt($timestamp);
    
    /**
     * @return int
     */
    public function getStatus();
    
    /**
     * @param int $status
     * @return $this
     */
    public function setStatus($status);
    
    /**
     * @return int
     */
    public function getRetries();
    
    /**
     * @param int $retries
     * @return $this
     */
    public function setRetries($retries);
    
    /**
     * @return string
     */
    public function getQueueName();
    
    /**
     * @param string $queueName
     * @return $this
     */
    public function setQueueName($queueName);
    
    /**
     * @return string
     */
    public function getMessageContent();
    
    /**
     * @param string $messageContent
     * @return $this
     */
    public function setMessageContent($messageContent);

    /**
     * @return string|null
     */
    public function getRunTaskAt();

    /**
     * @param string|null $runTaskAt
     * @return $this
     */
    public function setRunTaskAt($runTaskAt);

    /**
     * @return string|null
     */
    public function getLockedBy();

    /**
     * @param string|null $lockedBy
     * @return $this
     */
    public function setLockedBy($lockedBy);

    /**
     * @return string|null
     */
    public function getLockedUntil();

    /**
     * @param string|null $lockedUntil
     * @return $this
     */
    public function setLockedUntil($lockedUntil);
}
