-- Drift auf dem ZIEL erzeugen (nur für --check/--sync-Test):
--   db_skywings.flights : 17 Zeilen weg, 23 geändert, 9 zu viel  => 49 Diffs
--   db_airwings.payloads: 5 Zeilen weg, 11 geändert, 3 zu viel   => 19 Diffs

USE db_skywings;
DELETE FROM flights WHERE id BETWEEN 100 AND 116;
UPDATE flights SET price = price + 111.11, notes = CONCAT(COALESCE(notes,''), ' [drift]') WHERE id % 217 = 5 LIMIT 23;
INSERT INTO flights (flight_no, origin, dest, price, seats, notes, created)
SELECT CONCAT('XTRA', n), 'Nowhere', 'Void', 0.01, 1, 'nur auf Ziel', NOW()
FROM (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
      UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t;

USE db_airwings;
DELETE FROM payloads WHERE id BETWEEN 40 AND 44;
UPDATE payloads SET data = UNHEX('CAFEBABE00FF0A0D'), size_hint = 8 WHERE id % 73 = 7 LIMIT 11;
INSERT INTO payloads (label, data, meta, size_hint)
SELECT CONCAT('extra-', n), UNHEX('00112233'), NULL, 4
FROM (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3) t;
