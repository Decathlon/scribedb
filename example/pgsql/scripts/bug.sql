CREATE OR REPLACE FUNCTION md5_agg_sfunc(text, anyelement)
 RETURNS text LANGUAGE sql AS $$
 SELECT md5($1 || $2::text)
$$;

CREATE OR REPLACE AGGREGATE md5_agg (ORDER BY anyelement) (
 STYPE = text,
 SFUNC = md5_agg_sfunc,
 INITCOND = ''
);


DROP TABLE t_test cascade;

CREATE TABLE t_test (a int, b int, c text);

INSERT INTO t_test SELECT x, x + 10,'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' FROM generate_series(1, 4000000) AS x;

create or replace view v_test as
select a as a1,b as b1,a as a2,b as b2,a as a3,b as b3,a as a4,b as b4,c as c1,d as d1,e as e1,c as c2,d as d2,e as e2
from t_test;
