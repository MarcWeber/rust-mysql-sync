-- Drift auf dem ZIEL fuer db_range: je 2 geloescht, 3 geaendert, 1 zu viel.
USE db_range;

DELETE FROM char_keys WHERE code IN ('G000-001', 'G005-251');
UPDATE char_keys
   SET label = CONCAT(label, ' [drift]'), n = n + 10000
 WHERE code IN ('G001-051', 'G003-151', 'G011-551');
INSERT INTO char_keys (code, label, n) VALUES ('Z999-999', 'extra-char', 9999);

DELETE FROM binary_keys WHERE id IN (UNHEX('010100000001'), UNHEX('0106000000FB'));
UPDATE binary_keys
   SET label = CONCAT(label, ' [drift]'), n = n + 10000
 WHERE id IN (UNHEX('010200000033'), UNHEX('010400000097'), UNHEX('010C00000227'));
INSERT INTO binary_keys (id, label, n) VALUES (UNHEX('FF0000000999'), 'extra-binary', 9999);
