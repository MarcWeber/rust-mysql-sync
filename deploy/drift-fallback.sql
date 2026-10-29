-- Drift fuer automatische Mod-Fallback-Tabellen: je 1 geloescht, 1 geaendert, 1 zu viel.
USE db_range;

DELETE FROM date_keys WHERE d = '2024-01-02';
UPDATE date_keys SET v = 100 WHERE d = '2024-01-01';
INSERT INTO date_keys (d, v) VALUES ('2024-01-04', 4);

DELETE FROM composite_keys WHERE a = 1 AND b = 1;
UPDATE composite_keys SET v = 'changed' WHERE a = 1 AND b = 2;
INSERT INTO composite_keys (a, b, v) VALUES (2, 2, 'extra');
