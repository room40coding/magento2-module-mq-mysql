# Magento 2 Message Queue MySQL Backend
MySQL message queue backend implementation for [Rcason_Mq](https://github.com/renatocason/magento2-module-mq).

[![Build Status](https://travis-ci.org/renatocason/magento2-module-mq-mysql.svg?branch=master)](https://travis-ci.org/renatocason/magento2-module-mq-mysql)
[![Coverage Status](https://coveralls.io/repos/github/renatocason/magento2-module-mq-mysql/badge.svg?branch=master)](https://coveralls.io/github/renatocason/magento2-module-mq-mysql?branch=master)
[![Latest Stable Version](https://poser.pugx.org/renatocason/magento2-module-mq-mysql/v/stable)](https://packagist.org/packages/renatocason/magento2-module-mq-mysql)
[![Latest Unstable Version](https://poser.pugx.org/renatocason/magento2-module-mq-mysql/v/unstable)](https://packagist.org/packages/renatocason/magento2-module-mq-mysql)
[![Total Downloads](https://poser.pugx.org/renatocason/magento2-module-mq-mysql/downloads)](https://packagist.org/packages/renatocason/magento2-module-mq-mysql)
[![License](https://poser.pugx.org/renatocason/magento2-module-mq-mysql/license)](https://packagist.org/packages/renatocason/magento2-module-mq-mysql)

## Installation
1. Require the module via Composer
```bash
$ composer require renatocason/magento2-module-mq-mysql
```

2. Enable the module
```bash
$ bin/magento module:enable Rcason_MqMysql
$ bin/magento setup:upgrade
```

## Configuration
1. Configure the Mq module as explained [here](https://github.com/renatocason/magento2-module-mq)
2. Specify _mysql_ as broker when configuring a queue in your module's _etc/ce_mq.xml_ file
```xml
<?xml version="1.0"?>
<config xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="urn:magento:module:Rcason_Mq:etc/ce_mq.xsd">
    <ceQueue name="product.updates" broker="mysql"
        messageSchema="int"
        consumerInterface="Rcason\MqExample\Model\ExampleConsumer"/>
</config>
```

## Authors, contributors and maintainers

Author:
- [Renato Cason](https://github.com/renatocason)

## License
Licensed under the Open Software License version 3.0

## Concurrent consumers: claims and leases (2.1.0)

`peek()` is an **atomic claim**, not a read. Any number of consumers may drain the same queue — a
long-running `epicentre:mq:consume` worker next to a `ce_cron` tick, or several workers — and each
message is handed out once:

1. `claimNext()` selects the oldest runnable row (to-process, due, not under a live lease) and
   claims it with a compare-and-swap `UPDATE … WHERE <exactly the state it observed>`. Losing the race
   matches zero rows; the broker tries the next candidate (bounded) and otherwise returns `false`.
2. The claim writes `locked_by = host:pid:nonce` and `locked_until = UTC_TIMESTAMP() + lease`. The
   token rides on the envelope (`MysqlBroker::CLAIM_TOKEN_KEY`).
3. `acknowledge()` / `reject()` only act while the token still holds the row. If the lease lapsed and
   another consumer reclaimed the message they throw `Rcason\MqMysql\Exception\ClaimLostException`
   and change nothing — the other consumer owns the outcome now.
4. A reclaim of an expired lease counts as a retry. When that would reach the queue's `maxRetries`
   the message is dead-lettered (`status = 1`) instead of handed out, so a message that kills its
   consumer every time cannot circulate forever. Only the reclaim path can end a true crash loop: a
   consumer that dies never calls `reject()`. The parked row keeps the previous holder's token, so
   if that holder was merely slow and still finishes, its `acknowledge()` deletes the row and no
   dead letter is left for a message that was in fact processed.
5. Claiming does not touch `updated_at`: a message keeps its FIFO position and the age operators
   see. `reject(requeue)` does bump it, moving the message behind newer ones.

Per queue in `ce_mq.xml` (needs `room40coding/magento2-module-mq` ≥ 2.1 for the schema and the
`QueueAttributesInterface` the broker reads it through):

```xml
<ceQueue name="bombe.order.process" broker="mysql" … maxRetries="15" leaseSeconds="3600" />
```

`leaseSeconds` **must exceed the slowest `process()` on that queue** (size it for the worst case,
not the typical one; the schema rejects 0 and negatives, and a missing attribute means the default
900). Trade-off: a consumer killed mid-process (SIGKILL, OOM, supervisor's `stopwaitsecs` running
out) has its message redelivered only after the lease lapses, where the unlocked broker redelivered
immediately — and also to every other reader, which is the bug this closes. A runner that finishes
its current message on SIGTERM leaves no lease behind; one that is killed mid-message does.

Reading the table: `status = 0` with a live `locked_until` is in flight; `status = 1` with
`locked_by` set was parked at the reclaim cap while that holder still had it (check whether the
holder completed before replaying); `status = 1` with no lock was given up by `reject()`. To replay
a dead letter by hand clear the lock too:
`UPDATE ce_queue_message SET status = 0, retries = 0, run_task_at = NULL, locked_by = NULL, locked_until = NULL WHERE entity_id = ?`.

Schema: `locked_by VARCHAR(128) NULL`, `locked_until DATETIME NULL` (UpgradeSchema 0.1.2). All clock
comparisons are database-side `UTC_TIMESTAMP()`, so consumers on hosts with skewed clocks agree.

`MessageRepository::peek()`, `requeue()` and `remove()` remain for inspection-only callers but are
deprecated: they are not claim-aware.

Proof against a real database: `Test/Concurrency/race.php` (see its header).
