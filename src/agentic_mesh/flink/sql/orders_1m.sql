-- Orders placed per one-minute tumbling window, by event time.
INSERT INTO orders_1m
SELECT window_start, window_end, COUNT(*) AS orders
FROM TABLE(TUMBLE(TABLE order_placed, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;
