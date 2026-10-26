<?php
/**
 * Concurrency proof for the Rcason MySQL broker's atomic claim.
 *
 * Parent: seeds N messages on a scratch queue, spawns K worker processes that each drain it
 * (peek → record → acknowledge), then asserts every message was handed out exactly once and the
 * queue is empty — once for fresh messages, once for messages whose previous holder's lease has
 * expired (concurrent reclaim). Then single-process scenarios: lease expiry + reclaim, stale-token
 * acknowledge, two brokers in one process (the ce_cron + worker shape), requeue with backoff, retry
 * exhaustion, dead-letter at the reclaim cap with the holder's late acknowledge, stale-token requeue.
 *
 * Not a PHPUnit test: it needs a bootstrapped Magento with a real database (the claim is SQL).
 * Copy it under the app root and run inside the app container, e.g.
 *   cp vendor/room40coding/magento2-module-mq-mysql/Test/Concurrency/race.php var/race.php
 *   php var/race.php [--workers=6] [--messages=300]
 * Exit code 0 and "ALL PASSED" mean every scenario held.
 */
declare(strict_types=1);

use Magento\Framework\App\Bootstrap;
use Rcason\Mq\Model\MessageEnvelope;
use Rcason\MqMysql\Exception\ClaimLostException;
use Rcason\MqMysql\Model\MysqlBroker;
use Rcason\MqMysql\Model\ResourceModel\Queue\Message as MessageResource;

// App root: MAGENTO_ROOT, else the directory this script was copied under (var/), else cwd.
$root = getenv('MAGENTO_ROOT') ?: (is_file(dirname(__DIR__) . '/app/bootstrap.php') ? dirname(__DIR__) : getcwd());
require $root . '/app/bootstrap.php';
$bootstrap = Bootstrap::create(BP, $_SERVER);
$om = $bootstrap->getObjectManager();
$om->get(\Magento\Framework\App\State::class)->setAreaCode('crontab');

$opts = getopt('', ['worker::', 'workers::', 'messages::']);
$queue = 'zz.claim.race';
$outDir = sys_get_temp_dir() . '/ce-queue-race';
if (!is_dir($outDir)) { mkdir($outDir, 0777, true); }
/** @var MessageResource $resource */
$resource = $om->get(MessageResource::class);
$conn = $resource->getConnection();
$table = $resource->getMainTable();

$broker = static fn (int $lease = 900): MysqlBroker => $om->create(MysqlBroker::class, [
    'queueName' => $queue,
    'defaultLeaseSeconds' => $lease,
]);
$rowOf = static fn (int $id) => $conn->fetchRow($conn->select()->from($table)->where('entity_id = ?', $id));
$fail = static function (string $msg): void { fwrite(STDERR, "FAIL: $msg\n"); exit(1); };
$ok = static function (string $msg): void { echo "ok   $msg\n"; };
$expired = new \Zend_Db_Expr('UTC_TIMESTAMP() - INTERVAL 5 SECOND');
$deadHolder = 'dead:1:000000000000';

if (isset($opts['worker'])) {
    // ---- worker mode: drain until the queue hands out nothing, record every id we were given.
    $b = $broker();
    $fh = fopen("$outDir/worker-{$opts['worker']}.txt", 'w');
    while (($env = $b->peek()) !== false) {
        fwrite($fh, $env->getBrokerRef() . "\n");
        $b->acknowledge($env);
    }
    fclose($fh);
    exit(0);
}

$workers = (int) ($opts['workers'] ?? 6);
$messages = (int) ($opts['messages'] ?? 300);
$conn->delete($table, ['queue_name = ?' => $queue]);

/** Global UPDATE counter: every claim attempt is one UPDATE, so extra ones over the message count are lost CAS races. Shared server → approximate. */
$comUpdate = static function () use ($conn): ?int {
    try {
        $row = $conn->fetchRow("SHOW GLOBAL STATUS LIKE 'Com_update'");
        return $row ? (int) $row['Value'] : null;
    } catch (\Throwable $e) {
        return null;
    }
};

/** Seed N messages, let K workers drain them concurrently, assert exactly-once. */
$raceDrain = static function (string $label, callable $afterSeed) use (
    $broker, $conn, $table, $queue, $outDir, $workers, $messages, $fail, $ok, $comUpdate
): void {
    array_map('unlink', glob("$outDir/*.txt") ?: []);
    $seed = $broker();
    for ($i = 1; $i <= $messages; $i++) {
        $seed->enqueue((new MessageEnvelope())->setContent(json_encode(['n' => $i])));
    }
    $afterSeed();

    $updatesBefore = $comUpdate();
    $t0 = microtime(true);
    $procs = [];
    $pipes = [];
    for ($w = 1; $w <= $workers; $w++) {
        $procs[$w] = proc_open(['php', __FILE__, "--worker=$w"], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes[$w]);
    }
    $stderr = '';
    foreach ($procs as $w => $p) {
        $stderr .= stream_get_contents($pipes[$w][2]);
        proc_close($p);
    }
    $elapsed = microtime(true) - $t0;
    $updatesAfter = $comUpdate();
    if ($stderr !== '') { $fail("[$label] worker stderr: $stderr"); }

    $seen = [];
    $perWorker = [];
    foreach (glob("$outDir/worker-*.txt") as $f) {
        $ids = array_filter(array_map('trim', file($f)));
        $perWorker[basename($f)] = count($ids);
        foreach ($ids as $id) { $seen[(int) $id] = ($seen[(int) $id] ?? 0) + 1; }
    }
    $dupes = array_filter($seen, static fn ($c) => $c > 1);
    $total = array_sum($seen);
    $left = (int) $conn->fetchOne($conn->select()->from($table, 'COUNT(*)')->where('queue_name = ?', $queue));
    printf("race[%s]: %d workers drained %d messages in %.2fs — handed out %d, distinct %d, duplicates %d, left in table %d\n",
        $label, $workers, $messages, $elapsed, $total, count($seen), count($dupes), $left);
    printf("      per worker: %s\n", json_encode($perWorker));
    if ($updatesBefore !== null && $updatesAfter !== null) {
        printf("      UPDATE statements during the race: %d for %d claims → ≈%d lost CAS attempts (approximate: shared server)\n",
            $updatesAfter - $updatesBefore, $messages, max(0, $updatesAfter - $updatesBefore - $messages));
    }
    if ($dupes !== [] || count($seen) !== $messages || $total !== $messages || $left !== 0) {
        $fail("[$label] exactly-once violated");
    }
    $ok("[$label] every message claimed exactly once by exactly one worker; none lost; queue empty");
};

// ---- 1. K concurrent drainers, N fresh messages: exactly-once.
$raceDrain('fresh', static function (): void {});

// ---- 1b. Same, but every message is held under an expired lease by a dead consumer: concurrent reclaim.
$raceDrain('expired-leases', static function () use ($conn, $table, $queue, $deadHolder, $expired): void {
    $conn->update($table, ['locked_by' => $deadHolder, 'locked_until' => $expired], ['queue_name = ?' => $queue]);
});

// ---- 2. Lease expiry → reclaim bumps retries; stale token cannot acknowledge; claim keeps updated_at.
$a = $broker(1);
$a->enqueue((new MessageEnvelope())->setContent('{"lease":1}'));
$id = (int) $conn->fetchOne($conn->select()->from($table, 'MAX(entity_id)')->where('queue_name = ?', $queue));
$enqueuedAt = $rowOf($id)['updated_at'];
sleep(1);
$e1 = $a->peek();
if ((int) $e1->getBrokerRef() !== $id) { $fail('peek handed out an unexpected message'); }
$r = $rowOf($id);
if ($r['locked_by'] === null || (int) $r['retries'] !== 0) { $fail('claim did not set locked_by / retries'); }
if ($r['updated_at'] !== $enqueuedAt) { $fail("claim bumped updated_at ({$enqueuedAt} → {$r['updated_at']}); FIFO position and age must survive a claim"); }
if ($a->peek() !== false) { $fail('second peek handed out a message under a live lease'); }
$ok("live lease hides the message from a second peek (locked_by={$r['locked_by']}, until {$r['locked_until']}); updated_at untouched");
sleep(2);
$e2 = $a->peek();
if ($e2 === false || (int) $e2->getBrokerRef() !== $id) { $fail('expired lease was not reclaimed'); }
$r = $rowOf($id);
if ((int) $r['retries'] !== 1) { $fail("reclaim did not count as a retry (retries={$r['retries']})"); }
$ok('expired lease reclaimed with retries 0 → 1 and a fresh token');
try {
    $a->acknowledge($e1);
    $fail('stale token acknowledged someone else\'s claim');
} catch (ClaimLostException $ex) {
    $ok('stale-token acknowledge refused: ' . $ex->getMessage());
}
if ($rowOf($id) === false) { $fail('row vanished after refused ack'); }
$a->acknowledge($e2);
if ($rowOf($id) !== false) { $fail('current token failed to acknowledge'); }
$ok('current token acknowledged; row gone');

// ---- 3. Two brokers in one process (ce_cron + worker shape) over one message.
$x = $broker(); $y = $broker();
$x->enqueue((new MessageEnvelope())->setContent('{"two":1}'));
$ex1 = $x->peek();
if ($ex1 === false) { $fail('broker X got nothing'); }
if ($y->peek() !== false) { $fail('broker Y was handed the message X holds'); }
$x->acknowledge($ex1);
$ok('two brokers, one message: only one is handed the message');

// ---- 4. reject(requeue) releases with backoff; message not runnable until then; exhaustion dead-letters.
$z = $broker();
$z->enqueue((new MessageEnvelope())->setContent('{"requeue":1}'));
$ez = $z->peek();
$zid = (int) $ez->getBrokerRef();
$z->reject($ez, true, 3, 60);
$r = $rowOf($zid);
if ($r['locked_by'] !== null || (int) $r['retries'] !== 1 || (int) $r['status'] !== 0 || $r['run_task_at'] === null) {
    $fail('requeue did not release/backoff: ' . json_encode($r));
}
if ($z->peek() !== false) { $fail('backed-off message was handed out early'); }
$ok("reject(requeue) released the claim with backoff (retries=1, run_task_at={$r['run_task_at']})");
$conn->update($table, ['run_task_at' => null, 'retries' => 2], ['entity_id = ?' => $zid]);
$ez = $z->peek();
$z->reject($ez, true, 3, 60);
$r = $rowOf($zid);
if ((int) $r['status'] !== 1 || (int) $r['retries'] !== 3 || $r['locked_by'] !== null) { $fail('retry exhaustion did not dead-letter: ' . json_encode($r)); }
if ($z->peek() !== false) { $fail('dead-lettered message handed out'); }
$ok('third failure dead-lettered the message (status=1, lock cleared) and it is no longer handed out');

// ---- 5. Reclaim at the retry cap dead-letters instead of handing out (resource level, maxRetries=2),
//         keeps the holder's token, and the holder's late acknowledge still removes the row.
$conn->update($table, ['status' => 0, 'retries' => 1, 'run_task_at' => null, 'locked_by' => $deadHolder, 'locked_until' => $expired], ['entity_id = ?' => $zid]);
$claimed = $resource->claimNext($queue, 'me:1:aaaaaaaaaaaa', 60, 2);
$r = $rowOf($zid);
if ($claimed !== null || (int) $r['status'] !== 1 || (int) $r['retries'] !== 2 || $r['locked_by'] !== $deadHolder) {
    $fail('reclaim at cap did not dead-letter with the holder\'s token kept: ' . json_encode($r));
}
if ($resource->claimNext($queue, 'me:1:bbbbbbbbbbbb', 60, 2) !== null) { $fail('dead-lettered row handed out'); }
$ok("reclaiming an expired lease at the retry cap dead-letters instead of handing out; holder's token kept (locked_by={$r['locked_by']})");
if (!$resource->deleteClaimed($zid, 'me:1:aaaaaaaaaaaa')) { $ok('the reclaimer that parked it holds no claim and cannot delete it'); } else { $fail('reclaimer deleted a row it never held'); }
if (!$resource->deleteClaimed($zid, $deadHolder)) { $fail('the holder\'s late acknowledge was refused on a row parked at the cap'); }
if ($rowOf($zid) !== false) { $fail('row survived the holder\'s acknowledge'); }
$ok('the slow holder finished after all: its late acknowledge deleted the parked row, no dead letter left behind');

// ---- 6. Stale-token requeue at the SQL level changes nothing; the holder's token does.
$s = $broker();
$s->enqueue((new MessageEnvelope())->setContent('{"stale":1}'));
$es = $s->peek();
$sid = (int) $es->getBrokerRef();
$holder = $rowOf($sid)['locked_by'];
if ($resource->requeueClaimed($sid, 'stale:9:ffffffffffff', 3, 60)) { $fail('stale token requeued a row it does not hold'); }
$r = $rowOf($sid);
if ($r['locked_by'] !== $holder || (int) $r['retries'] !== 0 || (int) $r['status'] !== 0) { $fail('stale-token requeue changed the row: ' . json_encode($r)); }
$ok('stale-token requeueClaimed refused; row untouched');
if (!$resource->requeueClaimed($sid, $holder, 3, 60)) { $fail('holder could not requeue its own row'); }
$r = $rowOf($sid);
if ($r['locked_by'] !== null || (int) $r['retries'] !== 1) { $fail('holder requeue did not release: ' . json_encode($r)); }
$ok('holder requeueClaimed released the row (retries=1, lock cleared)');

$conn->delete($table, ['queue_name = ?' => $queue]);
echo "ALL PASSED\n";
