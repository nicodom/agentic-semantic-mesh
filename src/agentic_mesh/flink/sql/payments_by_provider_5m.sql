-- Payment attempts, failures and retries per provider per five-minute window, by event time.
INSERT INTO payments_by_provider_5m
SELECT
  window_start,
  window_end,
  provider,
  COUNT(*) AS attempts,
  COUNT(*) FILTER (WHERE status = 'failed') AS failures,
  CAST(COUNT(*) FILTER (WHERE status = 'failed') AS DOUBLE) / COUNT(*) AS failure_rate,
  COUNT(*) FILTER (WHERE attempt > 1) AS retries,
  COUNT(*) FILTER (WHERE attempt > 1 AND status = 'succeeded') AS retries_succeeded
FROM TABLE(TUMBLE(TABLE payment_attempted, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
GROUP BY window_start, window_end, provider;
