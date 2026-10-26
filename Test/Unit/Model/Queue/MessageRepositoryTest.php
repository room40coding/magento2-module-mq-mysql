<?php

namespace Rcason\MqMysql\Test\Unit\Model\Queue;

use Magento\Framework\Exception\NotFoundException;
use Magento\Framework\TestFramework\Unit\Helper\ObjectManager;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Rcason\MqMysql\Api\Data\QueueMessageInterface;
use Rcason\MqMysql\Api\Data\QueueMessageInterfaceFactory;
use Rcason\MqMysql\Api\QueueMessageRepositoryInterface;
use Rcason\MqMysql\Model\Queue\Message;
use Rcason\MqMysql\Model\Queue\MessageRepository;
use Rcason\MqMysql\Model\ResourceModel\Queue\Message as ResourceModel;
use Rcason\MqMysql\Model\ResourceModel\Queue\Message\Collection;
use Rcason\MqMysql\Model\ResourceModel\Queue\Message\CollectionFactory;

class MessageRepositoryTest extends TestCase
{
    private const QUEUE_NAME = 'test';
    private const MAX_RETRIES = 5;
    private const MESSAGE_ID = 7;
    private const TOKEN = 'host:1:abcdef012345';

    /** @var QueueMessageInterfaceFactory&MockObject */
    private $queueMessageFactory;

    /** @var ResourceModel&MockObject */
    private $resourceModel;

    /** @var CollectionFactory&MockObject */
    private $collectionFactory;

    /** @var Collection&MockObject */
    private $collection;

    private Message $message;

    private MessageRepository $messageRepository;

    protected function setUp(): void
    {
        $objectManager = new ObjectManager($this);

        $this->queueMessageFactory = $this->getMockBuilder(QueueMessageInterfaceFactory::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['create'])
            ->getMock();

        $this->collectionFactory = $this->getMockBuilder(CollectionFactory::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['create'])
            ->getMock();

        $this->resourceModel = $this->createMock(ResourceModel::class);
        $this->collection = $this->createMock(Collection::class);

        $this->message = $objectManager->getObject(Message::class);
        $this->messageRepository = $objectManager->getObject(MessageRepository::class, [
            'queueMessageFactory' => $this->queueMessageFactory,
            'resourceModel' => $this->resourceModel,
            'collectionFactory' => $this->collectionFactory,
        ]);

        parent::setUp();
    }

    public function testServiceContract(): void
    {
        $this->assertInstanceOf(QueueMessageRepositoryInterface::class, $this->messageRepository);
    }

    public function testCreate(): void
    {
        $this->resourceModel->expects($this->once())
            ->method('save')
            ->with($this->message);

        $this->messageRepository->create($this->message);
    }

    public function testClaimHydratesTheRowTheResourceModelClaimed(): void
    {
        $row = [
            'entity_id' => self::MESSAGE_ID,
            'queue_name' => self::QUEUE_NAME,
            'message_content' => '{"order_id":332}',
            'status' => QueueMessageInterface::STATUS_TO_PROCESS,
            'retries' => 1,
            'locked_by' => self::TOKEN,
            'locked_until' => '2026-10-26 05:00:00',
        ];
        $this->resourceModel->expects($this->once())
            ->method('claimNext')
            ->with(self::QUEUE_NAME, self::TOKEN, 300, self::MAX_RETRIES)
            ->willReturn($row);
        $this->queueMessageFactory->expects($this->once())
            ->method('create')
            ->willReturn($this->message);

        $claimed = $this->messageRepository->claim(self::QUEUE_NAME, self::TOKEN, 300, self::MAX_RETRIES);

        $this->assertSame($this->message, $claimed);
        $this->assertSame(self::MESSAGE_ID, $claimed->getEntityId());
        $this->assertSame('{"order_id":332}', $claimed->getMessageContent());
        $this->assertSame(self::TOKEN, $claimed->getLockedBy());
        $this->assertSame('2026-10-26 05:00:00', $claimed->getLockedUntil());
    }

    public function testClaimReturnsNullWhenNothingIsRunnable(): void
    {
        $this->resourceModel->method('claimNext')->willReturn(null);
        $this->queueMessageFactory->expects($this->never())->method('create');

        $this->assertNull($this->messageRepository->claim(self::QUEUE_NAME, self::TOKEN, 300));
    }

    public function testAcknowledgeDelegatesTheConditionalDelete(): void
    {
        $this->resourceModel->expects($this->once())
            ->method('deleteClaimed')
            ->with(self::MESSAGE_ID, self::TOKEN)
            ->willReturn(false);

        $this->assertFalse($this->messageRepository->acknowledge(self::MESSAGE_ID, self::TOKEN));
    }

    public function testRequeueClaimedDelegatesTheConditionalRelease(): void
    {
        $this->resourceModel->expects($this->once())
            ->method('requeueClaimed')
            ->with(self::MESSAGE_ID, self::TOKEN, self::MAX_RETRIES, 60)
            ->willReturn(true);

        $this->assertTrue($this->messageRepository->requeueClaimed(self::MESSAGE_ID, self::TOKEN, self::MAX_RETRIES, 60));
    }

    public function testPeek(): void
    {
        foreach (['addFieldToFilter', 'setOrder', 'setCurPage', 'setPageSize'] as $method) {
            $this->collection->method($method)->willReturn($this->collection);
        }
        $this->collection->expects($this->once())
            ->method('getFirstItem')
            ->willReturn($this->message);
        $this->collectionFactory->expects($this->once())
            ->method('create')
            ->willReturn($this->collection);

        $this->assertSame($this->message, $this->messageRepository->peek(self::QUEUE_NAME));
    }

    public function testGet(): void
    {
        $this->queueMessageFactory->expects($this->once())
            ->method('create')
            ->willReturn($this->message);
        $this->resourceModel->expects($this->once())
            ->method('load')
            ->with($this->message, self::MESSAGE_ID);
        $this->message->setId(self::MESSAGE_ID);

        $this->assertSame($this->message, $this->messageRepository->get(self::MESSAGE_ID));
    }

    public function testGetInvalid(): void
    {
        $this->expectException(NotFoundException::class);
        $this->messageRepository->get(0);
    }

    public function testGetNotFound(): void
    {
        $this->queueMessageFactory->expects($this->once())
            ->method('create')
            ->willReturn($this->message);
        $this->resourceModel->expects($this->once())
            ->method('load')
            ->with($this->message, self::MESSAGE_ID);

        $this->expectException(NotFoundException::class);
        $this->messageRepository->get(self::MESSAGE_ID);
    }

    public function testRequeue(): void
    {
        $retries = self::MAX_RETRIES - 2;
        $this->message->setStatus(QueueMessageInterface::STATUS_TO_PROCESS);
        $this->message->setRetries($retries);

        $this->messageRepository->requeue($this->message, 5, 0);

        $this->assertSame($retries + 1, $this->message->getRetries());
        $this->assertSame(QueueMessageInterface::STATUS_TO_PROCESS, $this->message->getStatus());
    }

    public function testRequeueLimit(): void
    {
        $retries = self::MAX_RETRIES;
        $this->message->setStatus(QueueMessageInterface::STATUS_TO_PROCESS);
        $this->message->setRetries($retries);

        $this->messageRepository->requeue($this->message, 5, 0);

        $this->assertSame($retries + 1, $this->message->getRetries());
        $this->assertSame(QueueMessageInterface::STATUS_MAX_RETRIES_EXCEEDED, $this->message->getStatus());
    }

    public function testRemove(): void
    {
        $this->resourceModel->expects($this->once())
            ->method('delete')
            ->with($this->message);

        $this->messageRepository->remove($this->message);
    }
}
