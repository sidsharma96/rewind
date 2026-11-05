-- Trusted operator setup for a NEW example project, before starting producers.
-- Select an explicitly installed, owned schema with psql -v schema=NAME.
-- No principal gains creation, writing or reading merely from its name.
BEGIN;
INSERT INTO :"schema".projects(id) VALUES(convert_to('demo','UTF8'));
SELECT id FROM :"schema".projects WHERE id=convert_to('demo','UTF8') FOR UPDATE;
INSERT INTO :"schema".runs(project,run_id,phase)
VALUES(convert_to('demo','UTF8'),convert_to('demo-run','UTF8'),'running');
INSERT INTO :"schema".host_creator_grants(project,run_id,instance_id,principal_id)
VALUES
(convert_to('demo','UTF8'),convert_to('demo-run','UTF8'),convert_to('alice','UTF8'),convert_to('host','UTF8')),
(convert_to('demo','UTF8'),convert_to('demo-run','UTF8'),convert_to('bob','UTF8'),convert_to('host','UTF8'));
INSERT INTO :"schema".writer_grants(project,run_id,instance_id,principal_id)
VALUES
(convert_to('demo','UTF8'),convert_to('demo-run','UTF8'),convert_to('alice','UTF8'),convert_to('alice','UTF8')),
(convert_to('demo','UTF8'),convert_to('demo-run','UTF8'),convert_to('bob','UTF8'),convert_to('bob','UTF8'));
INSERT INTO :"schema".reader_grants(project,principal_id)
VALUES(convert_to('demo','UTF8'),convert_to('reader','UTF8'));
COMMIT;
