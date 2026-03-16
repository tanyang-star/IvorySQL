CREATE EXTENSION mini_audit_pro;
CREATE TABLE users(id int, name text);
INSERT INTO users VALUES (1, 'a');
UPDATE users SET name = 'b' WHERE id = 1;
DELETE FROM users WHERE id = 1;
ALTER TABLE users ADD COLUMN note text;
DROP TABLE users;
SELECT op, schema_name, table_name FROM mini_audit_log ORDER BY id;
