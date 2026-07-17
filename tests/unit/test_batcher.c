#include "sentry_batcher.h"
#include "sentry_database.h"
#include "sentry_envelope.h"
#include "sentry_path.h"
#include "sentry_sync.h"
#include "sentry_testsupport.h"
#include "sentry_transport.h"
#include "sentry_value.h"

typedef struct {
    long started;
    long release;
} blocking_task_t;

static void
blocking_task_exec(void *data)
{
    blocking_task_t *task = data;
    sentry__atomic_store(&task->started, 1);
    while (!sentry__atomic_fetch(&task->release)) {
        sentry__thread_yield();
    }
}

static sentry_envelope_item_t *
pending_batch_func(sentry_envelope_t *envelope, sentry_value_t items)
{
    (void)items;
    return sentry__envelope_add_from_buffer(envelope, "{}", 2, "event");
}

static void
counting_transport_send(sentry_envelope_t *envelope, void *data)
{
    sentry__atomic_fetch_and_add((long *)data, 1);
    sentry_envelope_free(envelope);
}

static sentry_run_t *
new_test_run(const char *name, sentry_path_t **database_path)
{
    *database_path = sentry__path_from_str(name);
    TEST_ASSERT(!!*database_path);
    sentry__path_remove_all(*database_path);
    TEST_ASSERT(!sentry__path_create_dir_all(*database_path));
    sentry_run_t *run = sentry__run_new(*database_path);
    TEST_ASSERT(!!run);
    return run;
}

static void
free_test_run(sentry_run_t *run, sentry_path_t *database_path)
{
    sentry__run_clean(run, true);
    sentry__run_free(run);
    sentry__path_remove_all(database_path);
    sentry__path_free(database_path);
}

SENTRY_TEST(batcher_sync_flush_sends)
{
    sentry_batcher_t *batcher = sentry__batcher_new(
        pending_batch_func, SENTRY_DATA_CATEGORY_LOG_ITEM, NULL);
    TEST_ASSERT(!!batcher);
    sentry_path_t *database_path = NULL;
    sentry_run_t *run = new_test_run(
        SENTRY_TEST_PATH_PREFIX ".batcher-sync-flush", &database_path);
    long sent = 0;
    sentry_transport_t *transport
        = sentry_transport_new(counting_transport_send);
    TEST_ASSERT(!!transport);
    sentry_transport_set_state(transport, &sent);
    batcher->run = run;
    batcher->transport = transport;

    TEST_CHECK(sentry__batcher_enqueue(batcher, sentry_value_new_null()));
    TEST_CHECK(sentry__batcher_flush(batcher, false));
    TEST_CHECK_INT_EQUAL(sentry__atomic_fetch(&sent), 1);

    sentry__batcher_release(batcher);
    sentry_transport_free(transport);
    free_test_run(run, database_path);
}

SENTRY_TEST(batcher_enqueue_overflow)
{
    sentry_threadpool_t *pool = sentry__threadpool_new(1);
    TEST_ASSERT(!!pool);
    sentry_batcher_t *batcher = sentry__batcher_new(
        pending_batch_func, SENTRY_DATA_CATEGORY_LOG_ITEM, pool);
    TEST_ASSERT(!!batcher);

    for (int i = 0;
        i < SENTRY_BATCHER_BUFFER_COUNT * SENTRY_BATCHER_QUEUE_LENGTH; i++) {
        TEST_CHECK(sentry__batcher_enqueue(batcher, sentry_value_new_null()));
    }
    TEST_CHECK(!sentry__batcher_enqueue(batcher, sentry_value_new_null()));

    sentry__batcher_release(batcher);
    sentry__threadpool_free(pool);
}

SENTRY_TEST(batcher_force_flush_sends)
{
    sentry_threadpool_t *pool = sentry__threadpool_new(1);
    TEST_ASSERT(!!pool);
    TEST_ASSERT(!sentry__threadpool_start(pool));
    sentry_path_t *database_path = NULL;
    sentry_run_t *run = new_test_run(
        SENTRY_TEST_PATH_PREFIX ".batcher-force-flush", &database_path);
    long sent = 0;
    sentry_transport_t *transport
        = sentry_transport_new(counting_transport_send);
    TEST_ASSERT(!!transport);
    sentry_transport_set_state(transport, &sent);
    sentry_batcher_t *batcher = sentry__batcher_new(
        pending_batch_func, SENTRY_DATA_CATEGORY_LOG_ITEM, pool);
    TEST_ASSERT(!!batcher);
    batcher->run = run;
    batcher->transport = transport;

    TEST_CHECK(sentry__batcher_enqueue(batcher, sentry_value_new_null()));
    sentry__batcher_force_flush_begin(batcher);
    sentry__batcher_force_flush_wait(batcher);
    TEST_CHECK_INT_EQUAL(sentry__atomic_fetch(&sent), 1);

    sentry__batcher_release(batcher);
    sentry_transport_free(transport);
    sentry__threadpool_shutdown(pool);
    sentry__threadpool_free(pool);
    free_test_run(run, database_path);
}

SENTRY_TEST(batcher_crash_flush_buffers)
{
    sentry_threadpool_t *pool = sentry__threadpool_new(1);
    TEST_ASSERT(!!pool);
    sentry_path_t *database_path = NULL;
    sentry_run_t *run = new_test_run(
        SENTRY_TEST_PATH_PREFIX ".batcher-crash-flush", &database_path);
    sentry_batcher_t *batcher = sentry__batcher_new(
        pending_batch_func, SENTRY_DATA_CATEGORY_LOG_ITEM, pool);
    TEST_ASSERT(!!batcher);
    batcher->run = run;
    sentry__atomic_store(
        &batcher->thread_state, (long)SENTRY_BATCHER_THREAD_RUNNING);

    TEST_CHECK(sentry__batcher_enqueue(batcher, sentry_value_new_null()));
    sentry__batcher_flush_crash_safe(batcher);
    TEST_CHECK(sentry__atomic_fetch(&run->retain));

    sentry__batcher_release(batcher);
    sentry__threadpool_free(pool);
    free_test_run(run, database_path);
}

SENTRY_TEST(batcher_crash_flush_pending)
{
    sentry_path_t *database_path = sentry__path_from_str(
        SENTRY_TEST_PATH_PREFIX ".batcher-pending-crash-flush");
    TEST_ASSERT(!!database_path);
    sentry__path_remove_all(database_path);
    TEST_ASSERT(!sentry__path_create_dir_all(database_path));
    sentry_run_t *run = sentry__run_new(database_path);
    TEST_ASSERT(!!run);

    sentry_threadpool_t *pool = sentry__threadpool_new(1);
    TEST_ASSERT(!!pool);
    TEST_ASSERT(!sentry__threadpool_start(pool));

    blocking_task_t blocking_task = { 0 };
    TEST_ASSERT(!sentry__threadpool_submit(
        pool, blocking_task_exec, NULL, NULL, &blocking_task));
    while (!sentry__atomic_fetch(&blocking_task.started)) {
        sentry__thread_yield();
    }

    sentry_batcher_t *batcher = sentry__batcher_new(
        pending_batch_func, SENTRY_DATA_CATEGORY_LOG_ITEM, pool);
    TEST_ASSERT(!!batcher);
    long sent = 0;
    sentry_transport_t *transport
        = sentry_transport_new(counting_transport_send);
    TEST_ASSERT(!!transport);
    sentry_transport_set_state(transport, &sent);
    batcher->run = run;
    batcher->transport = transport;

    TEST_CHECK(sentry__batcher_enqueue(batcher, sentry_value_new_null()));
    TEST_CHECK(sentry__batcher_flush(batcher, false));
    TEST_CHECK(!sentry__atomic_fetch(&run->retain));

    sentry__batcher_flush_crash_safe(batcher);
    TEST_CHECK(sentry__atomic_fetch(&run->retain));

    sentry__atomic_store(&blocking_task.release, 1);
    sentry__threadpool_flush(pool);
    TEST_CHECK_INT_EQUAL(sentry__atomic_fetch(&sent), 0);
    sentry__threadpool_shutdown(pool);
    sentry__threadpool_free(pool);
    sentry__batcher_release(batcher);
    sentry_transport_free(transport);
    sentry__run_clean(run, true);
    sentry__run_free(run);
    sentry__path_remove_all(database_path);
    sentry__path_free(database_path);
}
