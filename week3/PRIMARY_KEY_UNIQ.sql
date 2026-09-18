CREATE SCEMA adhoc;

CREATE TABLE adhoc.primary_key_test (
  id int primary key
);

INSERT INTO adhoc.primary_key_test
VALUES (4), (4);

SELECT * FROM adhoc.primary_key_test;
