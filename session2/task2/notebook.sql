ADD JAR /home/cloudera/hive2.jar;
CREATE TEMPORARY FUNCTION agent_pars_udf as 'com.dstepanova.session2.task2.UserAgentFunc';
SELECT agent_pars_udf('Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405')


CREATE INDEX hive1_idx ON TABLE hive1 (streamId) AS 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX hive1_idx ON stream REBUILD;


WITH tt
AS (SELECT
  CAST(From_unixtime(Unix_timestamp(h1.TIME, 'yyyyMMddHHmmssSSS'),
  'yyyy-MM-dd')
  AS
  date) AS DATE,
  h1.ipinyouid AS pinid
FROM hive1 h1
WHERE h1.streamid = 11
GROUP BY From_unixtime(Unix_timestamp(h1.TIME, 'yyyyMMddHHmmssSSS'),
         'yyyy-MM-dd'),
         h1.ipinyouid
-- LIMIT 100
),
tt_new
AS (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  COUNT(tf.pinid)
  OVER (
  PARTITION BY tf.pinid
  ORDER BY tf.DATE ROWS UNBOUNDED PRECEDING) AS FIRST
FROM tt tf),
tt_cr
AS (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  COUNT(tf.pinid)
  OVER (
  PARTITION BY tf.pinid
  ORDER BY tf.DATE ROWS UNBOUNDED PRECEDING) AS FIRST
FROM tt tf),
tt_chr
AS (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  COUNT(tf.pinid)
  OVER (
  PARTITION BY tf.pinid
  ORDER BY tf.DATE ROWS UNBOUNDED PRECEDING) AS FIRST
FROM tt tf),
tt_ar
AS (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  COUNT(tf.pinid)
  OVER (
  PARTITION BY tf.pinid
  ORDER BY tf.DATE ROWS 3 PRECEDING) AS FIRST
FROM tt tf),
tt_arr
AS (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  COUNT(tf.pinid)
  OVER (
  PARTITION BY tf.pinid
  ORDER BY tf.DATE ROWS 3 PRECEDING) AS FIRST
FROM tt tf),
tt_l
AS (SELECT
  tf.DATE
  AS DATE,
  tf.pinid
  AS
  pinid,
  COUNT(tf.pinid)
  OVER (
  PARTITION BY tf.pinid
  ORDER BY tf.DATE ROWS BETWEEN 3 PRECEDING AND 3 PRECEDING)
  AS
  FIRST
FROM tt tf)
SELECT
  t.DATE,
  SUM(NEW.c),
  SUM(cr.c),
  SUM(chr.c),
  SUM(ar.c),
  SUM(arr.c)
FROM tt t
LEFT JOIN (SELECT
  NEW.DATE AS DATE,
  NEW.pinid AS pinid,
  1 AS C
FROM tt_new NEW
WHERE NEW.first = 1) NEW
  ON (NEW.DATE = t.DATE
  AND NEW.pinid = t.pinid)
LEFT JOIN (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  1 AS C
FROM tt_cr tf
WHERE tf.first = 2) cr
  ON (cr.DATE = t.DATE
  AND cr.pinid = t.pinid)
LEFT JOIN (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  1 AS C
FROM tt_chr tf
WHERE tf.first = 2) chr
  ON (chr.DATE = t.DATE
  AND chr.pinid = t.pinid)
LEFT JOIN (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  1 AS C
FROM tt_ar tf
WHERE tf.first = 1) ar
  ON (ar.DATE = t.DATE
  AND ar.pinid = t.pinid)
LEFT JOIN (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  1 AS C
FROM tt_arr tf
WHERE tf.first <= 2) arr
  ON (arr.DATE = t.DATE
  AND arr.pinid = t.pinid)
LEFT JOIN (SELECT
  tf.DATE AS DATE,
  tf.pinid AS pinid,
  1 AS C
FROM tt_l tf
WHERE tf.first <= 1) l
  ON (l.DATE = t.DATE
  AND l.pinid = t.pinid)
GROUP BY t.DATE






