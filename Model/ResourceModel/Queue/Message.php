<?php

namespace Rcason\MqMysql\Model\ResourceModel\Queue;

use Magento\Framework\Model\ResourceModel\Db\VersionControl\AbstractDb;
use Rcason\MqMysql\Api\Data\QueueMessageInterface;
use Zend_Db_Expr;

/**
 * `ce_queue_message` storage, plus the claim protocol that makes the queue safe for concurrent
 * consumers.
 *
 * Every mutation below is a single conditional statement whose WHERE pins the exact row state the
 * caller observed (compare-and-swap). Two consumers racing for one message therefore cannot both
 * win: the second UPDATE matches zero rows. All clock comparisons use the database's
 * UTC_TIMESTAMP() so consumers on hosts with skewed clocks agree on "due" and "expired".
 *
 * @SuppressWarnings(PHPMD.CamelCaseMethodName)
 */
class Message extends AbstractDb
{
    /**
     * How many candidates to try before giving up for this poll. Losing a race is rare and the next
     * poll retries anyway, so a small bound is enough to avoid spinning on a hot queue.
     */
    public const MAX_CLAIM_ATTEMPTS = 5;

    /**
     * @inheritdoc
     */
    protected function _construct()
    {
        $this->_init('ce_queue_message', 'entity_id');
    }

    /**
     * Claim the next runnable message of a queue.
     *
     * @param string $queueName
     * @param string $token Unique per claim; stored in locked_by.
     * @param int $leaseSeconds Lease length; at least one second.
     * @param int $maxRetries Reclaim cap: an expired-lease message whose next retry would reach this
     *                        is dead-lettered instead of handed out. 0 = no cap here.
     * @return array<string, string|int|null>|null The claimed row, or null when nothing is runnable.
     */
    public function claimNext(string $queueName, string $token, int $leaseSeconds, int $maxRetries = 0): ?array
    {
        $connection = $this->getConnection();
        $table = $this->getMainTable();

        for ($attempt = 0; $attempt < self::MAX_CLAIM_ATTEMPTS; $attempt++) {
            $candidate = $connection->fetchRow(
                $connection->select()
                    ->from($table, ['entity_id', 'locked_by', 'retries'])
                    ->where('queue_name = ?', $queueName)
                    ->where('status = ?', QueueMessageInterface::STATUS_TO_PROCESS)
                    ->where('run_task_at IS NULL OR run_task_at <= UTC_TIMESTAMP()')
                    ->where('locked_until IS NULL OR locked_until <= UTC_TIMESTAMP()')
                    ->order(['updated_at ASC', 'entity_id ASC'])
                    ->limit(1)
            );
            if (!$candidate) {
                return null;
            }

            $messageId = (int) $candidate['entity_id'];
            $observedOwner = $candidate['locked_by'];
            $observedRetries = (int) $candidate['retries'];
            // A token still on the row means its holder never acknowledged or requeued: it died or
            // overran its lease. Handing the message out again is a retry, and counts as one.
            $isReclaim = $observedOwner !== null;

            // Pin exactly what we saw. Any concurrent claim, requeue or acknowledge changes one of
            // these columns, so our UPDATE then matches nothing and we try the next candidate.
            $where = [
                'entity_id = ?' => $messageId,
                'status = ?' => QueueMessageInterface::STATUS_TO_PROCESS,
                'retries = ?' => $observedRetries,
                $observedOwner === null
                    ? 'locked_by IS NULL'
                    : $connection->quoteInto('locked_by = ?', $observedOwner),
                'locked_until IS NULL OR locked_until <= UTC_TIMESTAMP()',
            ];

            if ($isReclaim && self::exhausted($observedRetries + 1, $maxRetries)) {
                // Out of retries: park it rather than let a message that kills its consumer every
                // time circulate forever. The previous holder keeps its token on purpose: if it was
                // merely slow and still finishes, its acknowledge() deletes the row, so a message
                // that was in fact processed does not linger as a dead letter. Whether or not this
                // CAS wins, the row is no longer ours to hand out, so move on.
                $connection->update($table, [
                    'status' => QueueMessageInterface::STATUS_MAX_RETRIES_EXCEEDED,
                    'retries' => $observedRetries + 1,
                ], $where);
                continue;
            }

            $affected = $connection->update($table, [
                'locked_by' => $token,
                'locked_until' => self::utcPlusSeconds(max(1, $leaseSeconds)),
                'retries' => $observedRetries + ($isReclaim ? 1 : 0),
                // Claiming is not progress: keep the row's FIFO position and the age operators see.
                // An explicit assignment suppresses ON UPDATE CURRENT_TIMESTAMP.
                'updated_at' => new Zend_Db_Expr('updated_at'),
            ], $where);

            if ($affected === 1) {
                $row = $connection->fetchRow(
                    $connection->select()->from($table)->where('entity_id = ?', $messageId)
                );
                return $row ?: null;
            }
        }

        return null;
    }

    /**
     * Delete a message, but only while the given token still holds it.
     *
     * @return bool True when the row was deleted; false when the claim was lost.
     */
    public function deleteClaimed(int $messageId, string $token): bool
    {
        return $this->getConnection()->delete($this->getMainTable(), [
            'entity_id = ?' => $messageId,
            'locked_by = ?' => $token,
        ]) === 1;
    }

    /**
     * Release a held message back to the queue with backoff, or dead-letter it on the last retry.
     *
     * @param int $maxRetries 0 = unlimited
     * @param int $retryInterval seconds; 0 = runnable immediately
     * @return bool True when the row was updated; false when the claim was lost.
     */
    public function requeueClaimed(int $messageId, string $token, int $maxRetries, int $retryInterval): bool
    {
        $connection = $this->getConnection();
        $table = $this->getMainTable();

        // We hold the lease, so nobody else should be changing this row — but the lease may have
        // just expired, so the UPDATE below still pins the retries value we read here.
        $row = $connection->fetchRow(
            $connection->select()
                ->from($table, ['retries'])
                ->where('entity_id = ?', $messageId)
                ->where('locked_by = ?', $token)
        );
        if (!$row) {
            return false;
        }

        $observedRetries = (int) $row['retries'];
        $retries = $observedRetries + 1;

        return $connection->update($table, [
            'retries' => $retries,
            'status' => self::exhausted($retries, $maxRetries)
                ? QueueMessageInterface::STATUS_MAX_RETRIES_EXCEEDED
                : QueueMessageInterface::STATUS_TO_PROCESS,
            'run_task_at' => $retryInterval > 0 ? self::utcPlusSeconds($retryInterval) : null,
            'locked_by' => null,
            'locked_until' => null,
            // Explicit so the FIFO order (updated_at ASC) moves a requeued message behind newer ones
            // even on a connection whose ON UPDATE default is not in play.
            'updated_at' => new Zend_Db_Expr('CURRENT_TIMESTAMP'),
        ], [
            'entity_id = ?' => $messageId,
            'locked_by = ?' => $token,
            'retries = ?' => $observedRetries,
        ]) === 1;
    }

    /**
     * The one retry cap, shared by reclaim and requeue: dead-letter once the count reaches the cap.
     *
     * @param int $maxRetries 0 = no cap
     */
    private static function exhausted(int $retries, int $maxRetries): bool
    {
        return $maxRetries > 0 && $retries >= $maxRetries;
    }

    private static function utcPlusSeconds(int $seconds): Zend_Db_Expr
    {
        return new Zend_Db_Expr(sprintf('UTC_TIMESTAMP() + INTERVAL %d SECOND', $seconds));
    }
}
