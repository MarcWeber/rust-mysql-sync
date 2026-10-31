-- Seed für den mysql-sync End-to-End-Test.
-- Ausführen auf der QUELLE:  mysql -h <src> -u root < seed.sql
-- 2 Datenbanken:
--   db1 : Plain-Tabelle (kein Binary)
--   db2 : Tabelle mit BLOB (binärsicher testen: \0, \n, \t, 0xFF)

CREATE DATABASE IF NOT EXISTS db1 CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS db2 CHARACTER SET utf8mb4;

USE db1;
DROP TABLE IF EXISTS flights;
CREATE TABLE flights (
  id INT NOT NULL AUTO_INCREMENT,
  flight_no VARCHAR(12) NOT NULL,
  origin VARCHAR(64) NOT NULL,
  dest VARCHAR(64) NOT NULL,
  price DECIMAL(10,2) NULL,
  seats INT NOT NULL DEFAULT 180,
  notes TEXT NULL,
  created DATETIME NOT NULL,
  PRIMARY KEY (id),
  KEY idx_flight (flight_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5000 Zeilen mit heiklen Inhalten (Quotes, Tabs, Newlines, Unicode, NULLs)
INSERT INTO flights (flight_no, origin, dest, price, seats, notes, created)
SELECT
  CONCAT('SKW', LPAD(n, 5, '0')),
  CONCAT('City-', n % 97, CASE WHEN n % 13 = 0 THEN '''s End' ELSE '' END),
  CONCAT('Dest-', (n * 7) % 89),
  CASE WHEN n % 11 = 0 THEN NULL ELSE ROUND(19.99 + (n % 400) * 1.37, 2) END,
  60 + (n % 240),
  CASE
    WHEN n % 23 = 0 THEN NULL
    WHEN n % 23 = 1 THEN CONCAT('Zeile ', n, '\tMit\tTabs')
    WHEN n % 23 = 2 THEN CONCAT('Zeile ', n, '\nMit\nNewlines')
    WHEN n % 23 = 3 THEN CONCAT('Backslash ', n, '\\')
    ELSE CONCAT('Notiz äöüß Nr. ', n)
  END,
  DATE_ADD('2024-01-01 00:00:00', INTERVAL n MINUTE)
FROM (
  SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 + 1 AS n
  FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
       (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
       (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
       (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5) d
) nums
WHERE n <= 5000;

USE db2;
DROP TABLE IF EXISTS payloads;
CREATE TABLE payloads (
  id INT NOT NULL AUTO_INCREMENT,
  label VARCHAR(64) NOT NULL,
  data BLOB NULL,
  meta VARBINARY(64) NULL,
  size_hint INT NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 800 Zeilen mit echtem Binärmüll: \0, 0xFF, \n, \t, \\ — plus NULL-Blobs
INSERT INTO payloads (label, data, meta, size_hint)
SELECT label, data, meta, CASE WHEN data IS NULL THEN 0 ELSE LENGTH(data) END
FROM (
  SELECT
    CONCAT('payload-', n) AS label,
    CASE
      WHEN n % 17 = 0 THEN NULL
      WHEN n % 19 = 0 THEN UNHEX('000000')               -- exakt 3 Nullbytes (\0\0\0)
      WHEN n % 23 = 0 THEN UNHEX('FFFE80C0AF')            -- ungültiges UTF-8
      ELSE UNHEX(CONCAT(
        '00FF0A09200D5C',                               -- \0 0xFF \n \t space \r backslash
        LPAD(HEX(n), 8, '0'),
        REPEAT('AB', 40 + (n % 64)),                    -- variable Länge
        '0000FF'))
    END AS data,
    CASE
      WHEN n % 29 = 0 THEN NULL
      WHEN n % 31 = 0 THEN UNHEX('000000')               -- 3 Nullbytes in meta
      ELSE UNHEX(CONCAT('DEADBEEF', LPAD(HEX(n % 65536), 4, '0')))
    END AS meta
  FROM (
    SELECT a.N + b.N * 10 + c.N * 100 + 1 AS n
    FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8) c
  ) nums
  WHERE n <= 800
) t;

-- Range-Chunking: CHAR-/BINARY-PKs und absichtlich nicht unterstuetzte PK-Typen.
CREATE DATABASE IF NOT EXISTS db_range CHARACTER SET utf8mb4;
USE db_range;
DROP TABLE IF EXISTS char_keys;
CREATE TABLE char_keys (
  code VARCHAR(16) NOT NULL,
  label VARCHAR(64) NOT NULL,
  n INT NOT NULL,
  PRIMARY KEY (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO char_keys (code, label, n)
SELECT CONCAT('G', LPAD(FLOOR((n - 1) / 50), 3, '0'), '-', LPAD(n, 3, '0')),
       CONCAT('char-', n, CASE WHEN n % 7 = 0 THEN '\tTab' ELSE '' END),
       n
FROM (
  SELECT a.N + b.N * 10 + c.N * 100 + 1 AS n
  FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
       (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
       (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5) c
) nums
WHERE n <= 600;

DROP TABLE IF EXISTS binary_keys;
CREATE TABLE binary_keys (
  id VARBINARY(16) NOT NULL,
  label VARCHAR(64) NOT NULL,
  n INT NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO binary_keys (id, label, n)
SELECT UNHEX(CONCAT('01', LPAD(HEX(FLOOR((n - 1) / 50) + 1), 2, '0'), LPAD(HEX(n), 8, '0'))),
       CONCAT('binary-', n), n
FROM (
  SELECT a.N + b.N * 10 + c.N * 100 + 1 AS n
  FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
       (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
       (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5) c
) nums
WHERE n <= 600;

DROP TABLE IF EXISTS date_keys;
CREATE TABLE date_keys (
  d DATE NOT NULL,
  v INT NULL,
  PRIMARY KEY (d)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO date_keys (d, v) VALUES
  ('2024-01-01', 1), ('2024-01-02', 2), ('2024-01-03', 3);

DROP TABLE IF EXISTS composite_keys;
CREATE TABLE composite_keys (
  a INT NOT NULL,
  b INT NOT NULL,
  v VARCHAR(16) NULL,
  PRIMARY KEY (a, b)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO composite_keys (a, b, v) VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z');

DROP TABLE IF EXISTS no_pk;
CREATE TABLE no_pk (
  a INT NOT NULL,
  v VARCHAR(16) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO no_pk (a, v) VALUES (1, 'x'), (2, 'y');
