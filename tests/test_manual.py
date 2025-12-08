"""
MANUAL ONLY!

switch udp/tcp
turn on/off local agent during test 
"""

import threading
import time

import pytest
from statshouse import Client, DEFAULT_STATSHOUSE_ADDR

test_metric = "test_client"

class TestBasicMetrics:
    """Test basic metric types with specified names."""

    def test_count_udp(self):
        """Test counter metric via UDP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp")
        client.count(test_metric, {}, 1)
        client.count(test_metric, {}, 5)
        client.count(test_metric, {}, 10)

        time.sleep(1.5)
        client.close()

    def test_count_tcp(self):
        """Test counter metric via TCP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp")
        client.count(test_metric, {}, 1)
        client.count(test_metric, {}, 5)
        client.count(test_metric, {}, 10)

        time.sleep(1.5)
        client.close()

    def test_value_udp(self):
        """Test value metric via UDP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp")
        client.value(test_metric, {}, 1.0)
        client.value(test_metric, {}, [2.0, 3.0, 4.0])
        client.value(test_metric, {}, 5.0, n=10)

        time.sleep(1.5)
        client.close()

    def test_value_tcp(self):
        """Test value metric via TCP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp")
        client.value(test_metric, {}, 1.0)
        client.value(test_metric, {}, [2.0, 3.0, 4.0])
        client.value(test_metric, {}, 5.0, n=10)

        time.sleep(1.5)
        client.close()

    def test_uniq_udp(self):
        """Test unique metric via UDP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp")
        client.unique(test_metric, {}, 1)
        client.unique(test_metric, {}, [2, 3, 4, 5])
        client.unique(test_metric, {}, 6, n=100)

        time.sleep(1.5)
        client.close()

    def test_uniq_tcp(self):
        """Test unique metric via TCP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp")
        client.unique(test_metric, {}, 1)
        client.unique(test_metric, {}, [2, 3, 4, 5])
        client.unique(test_metric, {}, 6, n=100)

        time.sleep(1.5)
        client.close()


class TestBatching:
    """Test batching and periodic sending."""

    def test_batching_accumulation_udp(self):
        """Test that metrics are accumulated and sent in batches via UDP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp")

        for i in range(10):
            client.count(test_metric, {}, 1)
            time.sleep(1.5)
            client.value(test_metric, {}, float(i))
            time.sleep(1.5)
            client.unique(test_metric, {}, i)
        time.sleep(1.5)
        client.close()

    def test_batching_accumulation_tcp(self):
        """Test that metrics are accumulated and sent in batches via TCP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp")

        for i in range(10):
            client.count(test_metric, {}, 1)
            time.sleep(1.5)
            client.value(test_metric, {}, float(i))
            time.sleep(1.5)
            client.unique(test_metric, {}, i)
        time.sleep(1.5)
        client.close()

    def test_periodic_sending(self):
        """Test that metrics are sent periodically (every 1 second)."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp") # manual switch udp/tcp

        client.count(test_metric, {}, 1)
        time.sleep(1.5)
        client.count(test_metric, {}, 2)
        time.sleep(1.5)
        client.count(test_metric, {}, 3)
        time.sleep(1.5)
        client.count(test_metric, {}, 4)
        time.sleep(1.5)
        client.close()

    def test_multiple_metrics_same_bucket(self):
        """Test multiple metrics with same name+tags go to same bucket."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp") # manual switch udp/tcp

        # All these should accumulate in same bucket
        client.count(test_metric, {"tag1": "value1"}, 1)
        client.count(test_metric, {"tag1": "value1"}, 2)
        client.count(test_metric, {"tag1": "value1"}, 3)

        # Different tags = different bucket
        client.count(test_metric, {"tag1": "value2"}, 1)

        time.sleep(1.5)
        client.close()


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_large_batch(self):
        """Test sending large batch of metrics."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp") # manual switch udp/tcp
        for i in range(100):
            client.count(test_metric, {"id": str(i)}, 1)
        time.sleep(1.5)
        client.close()

    def test_reservoir_sampling(self):
        """Test reservoir sampling when bucket exceeds max_size."""
        client = Client(
            addr=DEFAULT_STATSHOUSE_ADDR,
            env="test",
            network="udp", # manual switch udp/tcp
            max_bucket_size=10  # Small size to trigger sampling
        )
        # Send more values than max_bucket_size
        for i in range(5000):
            client.value(test_metric, {}, float(i))
        time.sleep(1.5)
        client.close()

    def test_historic_metrics(self):
        """Test historic metrics (batched with regular metrics)."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp")  # manual switch udp/tcp
        past_ts = int(time.time()) - 3600
        
        # Historic metrics with ts - should be batched
        client.count(test_metric, {}, 100, ts=past_ts-0.1)
        client.value(test_metric, {}, 1.0, ts=past_ts+0.1)
        client.unique(test_metric, {}, 1, ts=past_ts)
        
        # Regular metrics without ts - should be batched together
        client.count(test_metric, {}, 1)
        client.value(test_metric, {}, 2.0)
        
        # All should be sent together in one batch
        time.sleep(1.5)
        client.close()

    def test_metrics_with_different_ts(self):
        """Test that metrics with different ts go to different buckets."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp")  # manual switch udp/tcp
        
        base_ts = int(time.time())
        
        # Metrics with different ts should be in different buckets
        client.count(test_metric, {}, 1, ts=base_ts - 100)
        client.count(test_metric, {}, 2, ts=base_ts - 200)
        client.count(test_metric, {}, 3, ts=base_ts - 300)
        
        # Regular metric without ts
        client.count(test_metric, {}, 4)
        
        time.sleep(1.5)
        client.close()

    def test_ts_rounding_to_seconds(self):
        """Test that ts is rounded to seconds (milliseconds/microseconds dropped)."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp")  # manual switch udp/tcp
        
        base_ts = int(time.time())
        
        # These should all go to same bucket (rounded to same second)
        client.count(test_metric, {}, 1, ts=base_ts + 0.123)
        client.count(test_metric, {}, 2, ts=base_ts + 0.456)
        client.count(test_metric, {}, 3, ts=base_ts + 0.789)
        client.count(test_metric, {}, 4, ts=base_ts)  # Exact second
        
        # This should go to different bucket (different second)
        client.count(test_metric, {}, 5, ts=base_ts + 1)
        
        time.sleep(1.5)
        client.close()

    def test_mixed_metrics_with_ts(self):
        """Test mixing metrics with and without ts in same batch."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp")  # manual switch udp/tcp
        
        past_ts = int(time.time()) - 60
        
        # Mix of metrics with ts and without ts
        client.count(test_metric, {}, 1)  # No ts
        client.count(test_metric, {}, 2, ts=past_ts)  # With ts
        client.value(test_metric, {}, 1.0)  # No ts
        client.value(test_metric, {}, 2.0, ts=past_ts)  # With ts
        client.unique(test_metric, {}, 1)  # No ts
        client.unique(test_metric, {}, 2, ts=past_ts)  # With ts
        
        # All should be batched together
        time.sleep(1.5)
        client.close()

    def test_same_ts_same_bucket(self):
        """Test that metrics with same ts go to same bucket."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp")  # manual switch udp/tcp
        
        same_ts = int(time.time()) - 60
        
        # All these should accumulate in same bucket
        client.count(test_metric, {}, 1, ts=same_ts-0.1)
        client.count(test_metric, {}, 2, ts=same_ts-0.03)
        client.count(test_metric, {}, 3, ts=same_ts-0.01)
        client.value(test_metric, {}, 1.0, ts=same_ts-0.05)
        client.value(test_metric, {}, 2.0, ts=same_ts-0.0222)
        client.unique(test_metric, {}, 1, ts=same_ts-0.23)
        client.unique(test_metric, {}, 2, ts=same_ts-0.467)
        
        time.sleep(1.5)
        client.close()


class TestBucketCleanup:
    """Test bucket cleanup logic. Debug"""

    def test_empty_bucket_cleanup(self):
        """Test that empty buckets are cleaned up after MAX_EMPTY_SEND_COUNT."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp") # manual switch udp/tcp

        # Create bucket with data
        client.count(test_metric, {}, 1)
        time.sleep(1.5)  # Send first batch

        # Bucket is now empty, but should not be removed yet
        time.sleep(1.5)  # Second empty send
        time.sleep(1.5)  # Third empty send (should trigger cleanup) [debug]

        client.close()

    def test_bucket_reuse(self):
        """Test that bucket is reused for same metric+tags."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp") # manual switch udp/tcp

        # Send metric
        client.count(test_metric, {}, 1)
        time.sleep(1.5)

        # Send again - should reuse same bucket
        client.count(test_metric, {}, 1)
        time.sleep(1.5)

        client.close()


class TestTCPEdgeCases:
    """Test TCP-specific edge cases."""

    def test_tcp_reconnect_on_error(self):
        """Test TCP reconnects on error."""
        # Client will use default logger (automatically configured)
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp") # turn off agent


        client.count(test_metric, {}, 1)
        time.sleep(2.0)
        time.sleep(3.0) # turn on agent

        client.count(test_metric, {}, 1)

        time.sleep(3.0)
        client.close()


    def test_tcp_queue_overflow(self):
        """Test TCP queue overflow (would block)."""
        # This is hard to test without actually filling the queue
        # But we can at least verify the code path exists
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp") # TCP_CONN_BUCKET_COUNT = 1!

        # Send many metrics quickly to potentially fill queue
        client.value(test_metric, {}, 1)
        time.sleep(2.0)
        client.value(test_metric, {}, 1)
        time.sleep(2.0)

        client.close()


class TestThreadSafety:
    """Test thread safety of client."""

    def test_concurrent_writes(self):
        """Test concurrent writes from multiple threads."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp")

        def write_metrics(thread_id):
            for i in range(10):
                client.count(test_metric, {"thread": str(thread_id)}, 1)
                client.value(test_metric, {"thread": str(thread_id)}, float(i))
                time.sleep(0.01)

        threads = []
        for i in range(5):
            t = threading.Thread(target=write_metrics, args=(i,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        time.sleep(1.5)
        client.close()

    def test_concurrent_different_metrics(self):
        """Test concurrent writes to different metrics."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp")

        def write_metric(metric_name):
            for i in range(10):
                client.count(metric_name, {}, 1)
                time.sleep(0.01)

        threads = []
        for i in range(5):
            t = threading.Thread(target=write_metric, args=(f"metric_{i}",))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        time.sleep(1.5)
        client.close()


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_udp_socket_error(self):
        """Test UDP socket errors."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp")

        # Close socket to simulate error
        if client.conn:
            client.conn.close()
            client.conn = None

        # Next send should recreate socket
        client.count(test_metric, {}, 1)
        time.sleep(1.5)
        client.close()


class TestLoadGenMetrics:
    """Comprehensive tests for loadgen metrics."""

    def test_loadgen_all_metrics_udp(self):
        """Test all loadgen metrics together via UDP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="loadgen", network="udp")

        # Counter
        for i in range(10):
            client.count("loadgen_const_cnt_1", {}, 1)

        # Value
        for i in range(10):
            client.value("loadgen_const_val_1", {}, float(i))

        # Unique
        for i in range(10):
            client.unique("loadgen_const_per_1", {}, i)

        time.sleep(1.5)
        client.close()

    def test_loadgen_all_metrics_tcp(self):
        """Test all loadgen metrics together via TCP."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="loadgen", network="tcp")

        # Counter
        for i in range(10):
            client.count("loadgen_const_cnt_1", {}, 1)

        # Value
        for i in range(10):
            client.value("loadgen_const_val_1", {}, float(i))

        # Unique
        for i in range(10):
            client.unique("loadgen_const_per_1", {}, i)

        time.sleep(1.5)
        client.close()

    def test_loadgen_high_volume(self):
        """Test loadgen metrics with high volume."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="loadgen", network="tcp")

        # High volume of metrics
        for i in range(100):
            client.count("loadgen_const_cnt_1", {}, 1)
            if i % 10 == 0:
                client.value("loadgen_const_val_1", {}, float(i))
            if i % 5 == 0:
                client.unique("loadgen_const_per_1", {}, i)

        time.sleep(1.5)
        client.close()

    def test_loadgen_with_tags(self):
        """Test loadgen metrics with various tags."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="loadgen", network="tcp")

        tags_variants = [
            {},
            {"instance": "server1"},
            {"instance": "server2", "region": "us-east"},
            ("server1", "us-east"),
            ["server1", "us-east", "prod"],
        ]

        for tags in tags_variants:
            client.count("loadgen_const_cnt_1", tags, 1)
            client.value("loadgen_const_val_1", tags, 1.0)
            client.unique("loadgen_const_per_1", tags, 1)

        time.sleep(1.5)
        client.close()

    def test_loadgen_historic(self):
        """Test loadgen metrics with historic timestamps."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="loadgen", network="tcp")

        past_ts = int(time.time()) - 3600

        client.count("loadgen_const_cnt_1", {}, 1, ts=past_ts)
        client.value("loadgen_const_val_1", {}, 1.0, ts=past_ts)
        client.unique("loadgen_const_per_1", {}, 1, ts=past_ts)

        # Historic metrics sent immediately
        time.sleep(0.2)
        client.close()


class TestStress:
    """Stress tests for high load scenarios."""

    def test_stress_many_values(self):
        """Stress test with many values in single metric."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="tcp")

        # Send many values
        values = [float(i) for i in range(1000)]
        client.value(test_metric, {}, values)

        time.sleep(1.5)
        client.close()

    def test_stress_many_uniques(self):
        """Stress test with many unique values."""
        client = Client(addr=DEFAULT_STATSHOUSE_ADDR, env="test", network="udp")

        # Send many unique values
        uniques = list(range(1000))
        client.unique(test_metric, {}, uniques)

        time.sleep(1.5)
        client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
