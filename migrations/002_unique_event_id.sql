-- Without this constraint, two concurrent requests for the same event_id
-- can both pass the application-level EventExists check and both insert,
-- producing duplicate rows, duplicate call records, and double-counted stats.
-- The UNIQUE constraint makes deduplication atomic at the database level.
ALTER TABLE events ADD CONSTRAINT events_event_id_unique UNIQUE (event_id);
