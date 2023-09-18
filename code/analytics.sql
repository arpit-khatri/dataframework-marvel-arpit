--showing the structure and stats of the table created for data analytics purpose.		
DESCRIBE FORMATTED ${db_name}.${table_name};

SELECT * FROM ${db_name}.${table_name} LIMIT 5;

SELECT count(1) total_heros,alignment FROM ${db_name}.${table_name} GROUP BY ALIGNMENT;