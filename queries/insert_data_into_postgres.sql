BEGIN;

DELETE FROM users WHERE id IN (SELECT id FROM staging_users);

INSERT INTO users SELECT * FROM staging_users;

TRUNCATE TABLE staging_users;

COMMIT;
