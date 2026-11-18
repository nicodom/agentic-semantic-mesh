-- Checkouts started and orders placed per five-minute window, and their ratio (ADR 0023: per
-- window, not per checkout cohort). Both streams go through one window so each window yields a
-- single row; the union keeps event_time as the time attribute, and its watermark is the
-- slower of the two.
CREATE TEMPORARY VIEW funnel_events AS
SELECT event_time, 1 AS is_checkout, 0 AS is_order FROM checkout_started
UNION ALL
SELECT event_time, 0 AS is_checkout, 1 AS is_order FROM order_placed;

INSERT INTO checkout_funnel_5m
SELECT
  window_start,
  window_end,
  CAST(SUM(is_checkout) AS BIGINT) AS checkouts_started,
  CAST(SUM(is_order) AS BIGINT) AS orders_placed,
  CASE WHEN SUM(is_checkout) > 0
    THEN CAST(SUM(is_order) AS DOUBLE) / SUM(is_checkout) END AS conversion
FROM TABLE(TUMBLE(TABLE funnel_events, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
GROUP BY window_start, window_end;
