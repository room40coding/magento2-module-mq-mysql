<?php

namespace Rcason\MqMysql\Test\Unit\Model;

use Magento\Framework\Exception\LocalizedException;
use Magento\Framework\TestFramework\Unit\Helper\ObjectManager;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Rcason\Mq\Api\BrokerInterface;
use Rcason\Mq\Api\Data\MessageEnvelopeInterfaceFactory;
use Rcason\Mq\Model\Config\Config;
use Rcason\Mq\Model\MessageEnvelope;
use Rcason\MqMysql\Api\Data\QueueMessageInterfaceFactory;
use Rcason\MqMysql\Api\QueueMessageRepositoryInterface;
use Rcason\MqMysql\Exception\ClaimLostException;
use Rcason\MqMysql\Model\MysqlBroker;
use Rcason\MqMysql\Model\Queue\Message;

class MysqlBrokerTest extends TestCase
{
    private const QUEUE_NAME = 'test_queue';
    private const MESSAGE_ID = 294;
    private const MESSAGE_CONTENT = 'Message content';
    private const OWNER = 'host:4242';

    /** @var QueueMessageInterfaceFactory&MockObject */
    private $queueMessageFactory;

    /** @var QueueMessageRepositoryInterface&MockObject */
    private $queueMessageRepository;

    /** @var Config&MockObject */
    private $queueConfig;

    private Message $message;

    private MysqlBroker $mysqlBroker;

    protected function setUp(): void
    {
        $objectManager = new ObjectManager($this);

        $this->queueMessageFactory = $this->getMockBuilder(QueueMessageInterfaceFactory::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['create'])
            ->getMock();

        // Real envelopes: the token travels on the DataObject, which is what the broker relies on.
        $messageEnvelopeFactory = $this->getMockBuilder(MessageEnvelopeInterfaceFactory::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['create'])
            ->getMock();
        $messageEnvelopeFactory->method('create')
            ->willReturnCallback(static fn () => new MessageEnvelope());

        $this->queueMessageRepository = $this->createMock(QueueMessageRepositoryInterface::class);
        $this->queueConfig = $this->createMock(Config::class);
        $this->message = $objectManager->getObject(Message::class);

        $this->mysqlBroker = $objectManager->getObject(MysqlBroker::class, [
            'queueMessageFactory' => $this->queueMessageFactory,
            'messageEnvelopeFactory' => $messageEnvelopeFactory,
            'queueMessageRepository' => $this->queueMessageRepository,
            'queueConfig' => $this->queueConfig,
            'queueName' => self::QUEUE_NAME,
            'ownerId' => self::OWNER,
        ]);

        parent::setUp();
    }

    public function testServiceContract(): void
    {
        $this->assertInstanceOf(BrokerInterface::class, $this->mysqlBroker);
    }

    public function testEnqueue(): void
    {
        $this->queueMessageFactory->expects($this->once())
            ->method('create')
            ->willReturn($this->message);

        $this->queueMessageRepository->expects($this->once())
            ->method('create')
            ->with($this->message);

        $envelope = (new MessageEnvelope())->setContent(self::MESSAGE_CONTENT);
        $this->mysqlBroker->enqueue($envelope);

        $this->assertSame(self::QUEUE_NAME, $this->message->getQueueName());
        $this->assertSame(self::MESSAGE_CONTENT, $this->message->getMessageContent());
    }

    public function testPeekClaimsWithTheQueuesConfiguredLeaseAndRetryCap(): void
    {
        $this->configureQueue(['leaseSeconds' => '120', 'maxRetries' => '7']);
        $this->message->setData(['entity_id' => self::MESSAGE_ID, 'message_content' => self::MESSAGE_CONTENT]);

        $this->queueMessageRepository->expects($this->once())
            ->method('claim')
            ->with(self::QUEUE_NAME, $this->matchesRegularExpression('/^host:4242:[0-9a-f]{12}$/'), 120, 7)
            ->willReturn($this->message);

        $envelope = $this->mysqlBroker->peek();

        $this->assertSame(self::MESSAGE_ID, $envelope->getBrokerRef());
        $this->assertSame(self::MESSAGE_CONTENT, $envelope->getContent());
        $this->assertMatchesRegularExpression('/^host:4242:[0-9a-f]{12}$/', $envelope->getData(MysqlBroker::CLAIM_TOKEN_KEY));
    }

    public function testPeekFallsBackToTheDefaultLeaseAndNoRetryCapWhenTheQueueSetsNone(): void
    {
        $this->configureQueue([]);

        $this->queueMessageRepository->expects($this->once())
            ->method('claim')
            ->with(self::QUEUE_NAME, $this->anything(), MysqlBroker::DEFAULT_LEASE_SECONDS, 0)
            ->willReturn(null);

        $this->assertFalse($this->mysqlBroker->peek());
    }

    public function testPeekReturnsFalseWhenNothingIsClaimable(): void
    {
        $this->configureQueue([]);
        $this->queueMessageRepository->method('claim')->willReturn(null);

        $this->assertFalse($this->mysqlBroker->peek());
    }

    public function testEveryPeekMintsADistinctToken(): void
    {
        $this->configureQueue([]);
        $this->message->setData(['entity_id' => self::MESSAGE_ID, 'message_content' => self::MESSAGE_CONTENT]);
        $this->queueMessageRepository->method('claim')->willReturn($this->message);

        $first = $this->mysqlBroker->peek()->getData(MysqlBroker::CLAIM_TOKEN_KEY);
        $second = $this->mysqlBroker->peek()->getData(MysqlBroker::CLAIM_TOKEN_KEY);

        $this->assertNotSame($first, $second);
    }

    public function testAcknowledgeDeletesTheRowOnlyUnderItsOwnToken(): void
    {
        $envelope = $this->claimedEnvelope();

        $this->queueMessageRepository->expects($this->once())
            ->method('acknowledge')
            ->with(self::MESSAGE_ID, $envelope->getData(MysqlBroker::CLAIM_TOKEN_KEY))
            ->willReturn(true);

        $this->mysqlBroker->acknowledge($envelope);
    }

    public function testAcknowledgeThrowsWhenTheClaimWasLost(): void
    {
        $envelope = $this->claimedEnvelope();
        $this->queueMessageRepository->method('acknowledge')->willReturn(false);

        $this->expectException(ClaimLostException::class);
        $this->expectExceptionMessage('message 294 before acknowledge');

        $this->mysqlBroker->acknowledge($envelope);
    }

    public function testRejectWithRequeueReleasesWithTheRunnersBackoff(): void
    {
        $envelope = $this->claimedEnvelope();

        $this->queueMessageRepository->expects($this->once())
            ->method('requeueClaimed')
            ->with(self::MESSAGE_ID, $envelope->getData(MysqlBroker::CLAIM_TOKEN_KEY), 15, 60)
            ->willReturn(true);
        $this->queueMessageRepository->expects($this->never())->method('acknowledge');

        $this->mysqlBroker->reject($envelope, true, 15, 60);
    }

    public function testRejectWithoutRequeueDeletesUnderItsOwnToken(): void
    {
        $envelope = $this->claimedEnvelope();

        $this->queueMessageRepository->expects($this->once())
            ->method('acknowledge')
            ->with(self::MESSAGE_ID, $envelope->getData(MysqlBroker::CLAIM_TOKEN_KEY))
            ->willReturn(true);
        $this->queueMessageRepository->expects($this->never())->method('requeueClaimed');

        $this->mysqlBroker->reject($envelope, false);
    }

    public function testRejectThrowsWhenTheClaimWasLost(): void
    {
        $envelope = $this->claimedEnvelope();
        $this->queueMessageRepository->method('requeueClaimed')->willReturn(false);

        $this->expectException(ClaimLostException::class);
        $this->expectExceptionMessage('before requeue');

        $this->mysqlBroker->reject($envelope, true, 15, 60);
    }

    public function testAnEnvelopeThatDidNotComeFromPeekIsRefused(): void
    {
        $foreign = (new MessageEnvelope())->setBrokerRef(self::MESSAGE_ID)->setContent(self::MESSAGE_CONTENT);
        $this->queueMessageRepository->expects($this->never())->method('acknowledge');

        $this->expectException(LocalizedException::class);
        $this->expectExceptionMessage('carries no claim token');

        $this->mysqlBroker->acknowledge($foreign);
    }

    /**
     * @param array<string, string> $attributes extra <ceQueue> attributes for the test queue
     */
    private function configureQueue(array $attributes): void
    {
        $this->queueConfig->method('get')
            ->with('queues', [])
            ->willReturn([
                ['name' => 'other_queue', 'broker' => 'mysql', 'leaseSeconds' => '5', 'maxRetries' => '99'],
                array_merge(['name' => self::QUEUE_NAME, 'broker' => 'mysql'], $attributes),
            ]);
    }

    private function claimedEnvelope(): MessageEnvelope
    {
        $this->configureQueue([]);
        $this->message->setData(['entity_id' => self::MESSAGE_ID, 'message_content' => self::MESSAGE_CONTENT]);
        $this->queueMessageRepository->method('claim')->willReturn($this->message);

        /** @var MessageEnvelope $envelope */
        $envelope = $this->mysqlBroker->peek();

        return $envelope;
    }
}
