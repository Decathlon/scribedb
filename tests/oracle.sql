CREATE OR REPLACE TYPE numbers_t AS TABLE OF NUMBER;
/

CREATE OR REPLACE FUNCTION generate_series (minnumber INTEGER, maxnumber INTEGER)
   RETURN numbers_t
   PIPELINED
   DETERMINISTIC
IS
BEGIN
   FOR i IN minnumber .. maxnumber LOOP
      PIPE ROW (i);
   END LOOP;
   RETURN;
END;
/
alter session set current_schema=system;
DROP TABLE t_test;
CREATE TABLE t_test (a number, b number, c varchar2(255));
INSERT INTO t_test SELECT column_value, column_value + 10,'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' FROM table(generate_series(1, 50000));

commit;
