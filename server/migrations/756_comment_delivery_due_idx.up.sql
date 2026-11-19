CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comment_delivery_due ON comment_delivery (next_attempt_at, comment_id) WHERE next_attempt_at IS NOT NULL;
