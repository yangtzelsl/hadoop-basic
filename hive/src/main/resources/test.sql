-- 测试SQL
-- 第一个参数:broker所在位置
-- 第二个参数:topic
-- 第三个参数:将多行转成集合,每一行转成一个map

SELECT g,hive2kafka('bd01:9092','bobizli_test',collect_list(map('full_name',full_name,'simple_name',simple_name))) AS result
FROM
(
SELECT r1,pmod(ABS(hash(r1)),1000) AS g,full_name,simple_name
FROM(
SELECT row_number() over(PARTITION BY 1) AS r1,full_name,simple_name
FROM dws_bo_final_spider_contact
LIMIT 10000) tmp
) tmp2
GROUP BY g;
