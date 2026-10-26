<?php

namespace Rcason\MqMysql\Model;

use Magento\Framework\Exception\LocalizedException;
use Rcason\Mq\Api\Config\ConfigInterface;
use Rcason\Mq\Api\Data\MessageEnvelopeInterface;
use Rcason\Mq\Api\Data\MessageEnvelopeInterfaceFactory;
use Rcason\MqMysql\Api\Data\QueueMessageInterfaceFactory;
use Rcason\MqMysql\Api\QueueMessageRepositoryInterface;
use Rcason\MqMysql\Exception\ClaimLostException;

/**
 * MySQL-backed broker. peek() is an atomic claim, so any number of consumers may drain the same
 * queue (a long-running worker alongside a cron drainer, several workers) and each message is
 * processed once.
 *
 * A claim is a per-message lease. acknowledge()/reject() act only while the lease is still ours;
 * once it has lapsed and another consumer has taken the message, they throw ClaimLostException
 * instead of touching the other consumer's row. The lease must therefore outlast the slowest
 * process() on the queue: configure it per queue with the `leaseSeconds` attribute in ce_mq.xml,
 * or rely on the default below.
 */
class MysqlBroker implements \Rcason\Mq\Api\BrokerInterface
{
    /**
     * Envelope data key carrying the claim token from peek() to acknowledge()/reject().
     * The envelope interface has no slot for it; MessageEnvelope is a DataObject.
     */
    public const CLAIM_TOKEN_KEY = 'mysql_broker_claim_token';

    /** Fifteen minutes: generous for a SYSPRO push with several splits, short enough to recover a dead worker. */
    public const DEFAULT_LEASE_SECONDS = 900;

    /** ce_mq.xml <ceQueue> attribute names read per queue. */
    private const ATTR_LEASE_SECONDS = 'leaseSeconds';
    private const ATTR_MAX_RETRIES = 'maxRetries';

    /**
     * @var QueueMessageInterfaceFactory
     */
    protected $queueMessageFactory;
    
    /**
     * @var MessageEnvelopeInterfaceFactory
     */
    protected $messageEnvelopeFactory;
    
    /**
     * @var QueueMessageRepositoryInterface
     */
    protected $queueMessageRepository;
    
    /**
     * @var string
     */
    protected $queueName;

    /**
     * @var ConfigInterface
     */
    private $queueConfig;

    /**
     * @var int
     */
    private $defaultLeaseSeconds;

    /**
     * Identifies this consumer process in locked_by (`host:pid`), for operators reading the table.
     *
     * @var string
     */
    private $ownerId;
    
    /**
     * @param QueueMessageInterfaceFactory $queueMessageFactory
     * @param MessageEnvelopeInterfaceFactory $messageEnvelopeFactory
     * @param QueueMessageRepositoryInterface $queueMessageRepository
     * @param ConfigInterface $queueConfig
     * @param string|null $queueName
     * @param int $defaultLeaseSeconds
     * @param string|null $ownerId
     */
    public function __construct(
        QueueMessageInterfaceFactory $queueMessageFactory,
        MessageEnvelopeInterfaceFactory $messageEnvelopeFactory,
        QueueMessageRepositoryInterface $queueMessageRepository,
        ConfigInterface $queueConfig,
        $queueName = null,
        int $defaultLeaseSeconds = self::DEFAULT_LEASE_SECONDS,
        ?string $ownerId = null
    ) {
        $this->queueMessageFactory = $queueMessageFactory;
        $this->messageEnvelopeFactory = $messageEnvelopeFactory;
        $this->queueMessageRepository = $queueMessageRepository;
        $this->queueConfig = $queueConfig;
        $this->queueName = $queueName;
        $this->defaultLeaseSeconds = $defaultLeaseSeconds;
        $this->ownerId = $ownerId ?? ((string) gethostname() . ':' . getmypid());
    }
    
    /**
     * @inheritdoc
     */
    public function enqueue(MessageEnvelopeInterface $messageEnvelope)
    {
        $queueMessage = $this->queueMessageFactory->create()
            ->setQueueName($this->queueName)
            ->setMessageContent($messageEnvelope->getContent());
        
        return $this->queueMessageRepository->create($queueMessage);
    }
    
    /**
     * Claim the next runnable message. Despite the name this is not a read: the message is
     * leased to this consumer until acknowledge()/reject() or the lease expires.
     *
     * @inheritdoc
     */
    public function peek()
    {
        $token = $this->newClaimToken();
        $queueMessage = $this->queueMessageRepository->claim(
            (string) $this->queueName,
            $token,
            $this->leaseSeconds(),
            $this->maxRetries()
        );
        if (!$queueMessage || !$queueMessage->getEntityId()) {
            return false;
        }
        
        return $this->messageEnvelopeFactory->create()
            ->setBrokerRef($queueMessage->getEntityId())
            ->setContent($queueMessage->getMessageContent())
            ->setData(self::CLAIM_TOKEN_KEY, $token);
    }
    
    /**
     * @inheritdoc
     * @throws ClaimLostException When the lease lapsed and the message is no longer ours.
     */
    public function acknowledge(MessageEnvelopeInterface $message)
    {
        $messageId = $this->messageIdOf($message);
        if (!$this->queueMessageRepository->acknowledge($messageId, $this->claimTokenOf($message))) {
            throw $this->claimLost($messageId, 'acknowledge');
        }
    }
    
    /**
     * @inheritdoc
     * @throws ClaimLostException When the lease lapsed and the message is no longer ours.
     */
    public function reject(MessageEnvelopeInterface $message, bool $requeue, int $maxRetries = 0, int $retryInterval = 0)
    {
        $messageId = $this->messageIdOf($message);
        $token = $this->claimTokenOf($message);

        $released = $requeue
            ? $this->queueMessageRepository->requeueClaimed($messageId, $token, $maxRetries, $retryInterval)
            : $this->queueMessageRepository->acknowledge($messageId, $token);

        if (!$released) {
            throw $this->claimLost($messageId, $requeue ? 'requeue' : 'reject');
        }
    }

    /**
     * Lease for this queue: ce_mq.xml `leaseSeconds` when set, else the broker default.
     */
    public function leaseSeconds(): int
    {
        $configured = (int) ($this->queueSetting(self::ATTR_LEASE_SECONDS) ?? 0);

        return $configured > 0 ? $configured : $this->defaultLeaseSeconds;
    }

    /**
     * Retry cap applied when an expired lease is reclaimed: ce_mq.xml `maxRetries`, 0 = none.
     */
    public function maxRetries(): int
    {
        return max(0, (int) ($this->queueSetting(self::ATTR_MAX_RETRIES) ?? 0));
    }

    /**
     * Unique per claim and readable by a human: `host:pid:nonce`.
     */
    private function newClaimToken(): string
    {
        return $this->ownerId . ':' . bin2hex(random_bytes(6));
    }

    private function messageIdOf(MessageEnvelopeInterface $message): int
    {
        return (int) $message->getBrokerRef();
    }

    /**
     * @throws LocalizedException When the envelope did not come from this broker's peek().
     */
    private function claimTokenOf(MessageEnvelopeInterface $message): string
    {
        $token = method_exists($message, 'getData') ? $message->getData(self::CLAIM_TOKEN_KEY) : null;
        if (!is_string($token) || $token === '') {
            throw new LocalizedException(__(
                'Message %1 on queue %2 carries no claim token: only envelopes returned by peek() can be acknowledged or rejected',
                $this->messageIdOf($message),
                $this->queueName
            ));
        }

        return $token;
    }

    private function claimLost(int $messageId, string $operation): ClaimLostException
    {
        return new ClaimLostException(__(
            'Claim lost on %1 message %2 before %3: its lease expired and the message is now held by another consumer or gone. Nothing was changed.',
            $this->queueName,
            $messageId,
            $operation
        ));
    }

    /**
     * Read one of this queue's <ceQueue> attributes. The Rcason config object keeps every
     * attribute of the merged ce_mq.xml under 'queues' (same fallback CeCron uses).
     *
     * @return string|int|null
     */
    private function queueSetting(string $attribute)
    {
        if (!method_exists($this->queueConfig, 'get')) {
            return null;
        }

        foreach ((array) $this->queueConfig->get('queues', []) as $queue) {
            if (($queue['name'] ?? null) === $this->queueName) {
                return $queue[$attribute] ?? null;
            }
        }

        return null;
    }
}
