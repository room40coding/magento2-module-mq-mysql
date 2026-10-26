<?php

namespace Rcason\MqMysql\Api;

use Magento\Framework\Exception\NotFoundException;
use Rcason\MqMysql\Api\Data\QueueMessageInterface;

interface QueueMessageRepositoryInterface
{
    /**
     * Persist a new message.
     *
     * @param QueueMessageInterface $message
     * @return void
     */
    public function create(QueueMessageInterface $message);

    /**
     * Atomically claim the next runnable message of a queue for this consumer.
     *
     * Runnable = to-process, due (run_task_at), and not held under a live lease. The claim is a
     * compare-and-swap on the row's observed state, so concurrent consumers (a dedicated worker
     * and a cron drainer, or several workers) can never hand out the same message twice. A
     * message whose previous holder's lease expired is handed out again with its retry count
     * incremented, or dead-lettered when that would reach $maxRetries (0 = no cap at reclaim).
     *
     * @param string $queueName
     * @param string $token Unique per claim; required again by acknowledge() / requeueClaimed().
     * @param int $leaseSeconds How long the claim holds before another consumer may take the message.
     * @param int $maxRetries Retry cap applied when reclaiming an expired lease; 0 disables it.
     * @return QueueMessageInterface|null Null when nothing is runnable right now.
     */
    public function claim(string $queueName, string $token, int $leaseSeconds, int $maxRetries = 0);

    /**
     * Remove a message this token still holds (successful processing, or reject without requeue).
     *
     * @param int $messageId
     * @param string $token
     * @return bool False when the claim was lost: the lease expired and the row is gone or held by
     *              another consumer. Nothing is deleted in that case.
     */
    public function acknowledge(int $messageId, string $token): bool;

    /**
     * Return a held message to the queue with backoff, or dead-letter it once retries are exhausted.
     *
     * @param int $messageId
     * @param string $token
     * @param int $maxRetries 0 = unlimited
     * @param int $retryInterval Seconds before the message is runnable again; 0 = immediately
     * @return bool False when the claim was lost (see acknowledge()).
     */
    public function requeueClaimed(int $messageId, string $token, int $maxRetries, int $retryInterval): bool;

    /**
     * Read the next runnable message WITHOUT claiming it.
     *
     * @deprecated Unsafe with more than one consumer on a queue: two readers see the same row and
     *             both process it. Use claim(). Kept for callers that only inspect the queue.
     * @param string $queueName
     * @return QueueMessageInterface
     */
    public function peek(string $queueName);
    
    /**
     * @param int $id
     * @return QueueMessageInterface
     * @throws NotFoundException
     */
    public function get($id);
    
    /**
     * @deprecated Not claim-aware; use requeueClaimed().
     * @param QueueMessageInterface $message
     * @param int $maxRetries
     * @param int $retryInterval
     * @return void
     */
    public function requeue(QueueMessageInterface $message, int $maxRetries, int $retryInterval);
    
    /**
     * @deprecated Not claim-aware; use acknowledge().
     * @param QueueMessageInterface $message
     * @return void
     */
    public function remove(QueueMessageInterface $message);
}
