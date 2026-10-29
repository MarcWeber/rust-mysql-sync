-- Drift auf dem ZIEL erzeugen (nur für --check/--sync-Test):
--   db_skywings.flights : 17 Zeilen weg, 23 geändert, 9 zu viel  => 49 Diffs
--   db_airwings.payloads: 5 Zeilen weg, 11 geändert, 3 zu viel   => 19 Diffs
-- Die Zeilen werden zufällig gewählt (ORDER BY RAND()), die Anzahl ist fix,
-- dadurch variiert der Test zwischen Läufen, die erwarteten Diff-Zahlen bleiben.

USE db_skywings;
DELETE FROM flights ORDER BY RAND() LIMIT 17;
UPDATE flights SET price = price + 111.11, notes = CONCAT(COALESCE(notes,''), ' [drift]') ORDER BY RAND() LIMIT 23;
INSERT INTO flights (flight_no, origin, dest, price, seats, notes, created)
SELECT CONCAT('XTRA', n), 'Nowhere', 'Void', 0.01, 1, 'nur auf Ziel', NOW()
FROM (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
      UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t;

USE db_airwings;
DELETE FROM payloads ORDER BY RAND() LIMIT 5;
UPDATE payloads SET data = UNHEX('CAFEBABE00FF0A0D000000'), size_hint = 10 ORDER BY RAND() LIMIT 11;
INSERT INTO payloads (label, data, meta, size_hint)
SELECT CONCAT('extra-', n), UNHEX('0011223300FF'), NULL, 6
FROM (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3) t;
