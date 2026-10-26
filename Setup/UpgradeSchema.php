<?php

namespace Rcason\MqMysql\Setup;

use Magento\Framework\DB\Ddl\Table;
use Magento\Framework\Setup\UpgradeSchemaInterface;
use Magento\Framework\Setup\ModuleContextInterface;
use Magento\Framework\Setup\SchemaSetupInterface;

/**
 * @codeCoverageIgnore
 */
class UpgradeSchema implements  UpgradeSchemaInterface
{
    /**
     * @inheritdoc
     */
    public function upgrade(SchemaSetupInterface $setup, ModuleContextInterface $context)
    {
        $setup->startSetup();

        if (version_compare($context->getVersion(), '0.1.1') < 0) {
            $setup->getConnection()->addColumn(
                $setup->getTable('ce_queue_message'),
                'run_task_at',
                [
                    'type' => Table::TYPE_DATETIME,
                    'nullable' => true,
                    'comment' => 'Do not run task before this date and time'
                ]
            );
        }

        if (version_compare($context->getVersion(), '0.1.2') < 0) {
            // Claim lease: who holds the message and until when (UTC). See ResourceModel\Queue\Message.
            $setup->getConnection()->addColumn(
                $setup->getTable('ce_queue_message'),
                'locked_by',
                [
                    'type' => Table::TYPE_TEXT,
                    'length' => 128,
                    'nullable' => true,
                    'comment' => 'Claim token of the consumer holding the message (host:pid:nonce)'
                ]
            );
            $setup->getConnection()->addColumn(
                $setup->getTable('ce_queue_message'),
                'locked_until',
                [
                    'type' => Table::TYPE_DATETIME,
                    'nullable' => true,
                    'comment' => 'UTC expiry of the current claim; runnable again after this'
                ]
            );
        }

        $setup->endSetup();
    }
}
