<?php

namespace Rcason\MqMysql\Exception;

use Magento\Framework\Exception\LocalizedException;

/**
 * Thrown by the broker when acknowledge()/reject() find the consumer no longer holds the message:
 * its lease expired while process() ran and another consumer has claimed it since (or it is gone).
 *
 * The right reaction is to do nothing further with the message — the other consumer owns the
 * outcome now. The exception exists so the condition is loud in logs rather than silently
 * deleting or requeueing a row that belongs to someone else.
 */
class ClaimLostException extends LocalizedException
{
}
