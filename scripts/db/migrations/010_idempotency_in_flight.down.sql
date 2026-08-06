DROP INDEX IF EXISTS processed_idempotency_keys_in_flight_age;

-- Drop the state column. Going back means in_flight rows would have
-- NULL response data and become un-replayable; the safest reverse
-- is to delete them first.
DELETE FROM processed_idempotency_keys WHERE state = 'in_flight';

ALTER TABLE processed_idempotency_keys
    DROP COLUMN IF EXISTS state,
    ALTER COLUMN response_status SET NOT NULL,
    ALTER COLUMN response_body SET NOT NULL,
    ALTER COLUMN response_content_type SET NOT NULL;
