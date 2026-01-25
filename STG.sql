CREATE TABLE stg.stg_account (
    -- Данные источника
    account_id      VARCHAR2(16),
    account_name    VARCHAR2(50),
    start_balance   NUMBER(10,2),
    
    -- Технические метаданные
    load_date       DATE DEFAULT SYSDATE,       -- Дата загрузки в STG
    record_source   VARCHAR2(50) DEFAULT 'SRC.ACCOUNT', -- Источник данных
    hash_diff       VARCHAR2(64) -- Хэш всех бизнес-атрибутов (для обнаружения изменений)
);

CREATE TABLE stg.stg_transaction (
    -- Данные источника
    transaction_id      NUMBER(10),
    transaction_date    DATE,
    debit_account_id    VARCHAR2(16),
    credit_account_id   VARCHAR2(16),
    amount              NUMBER(10,2),
    
    -- Технические метаданные
    load_date       DATE DEFAULT SYSDATE,
    record_source   VARCHAR2(50) DEFAULT 'SRC.TRANSACTION',
    hash_diff       VARCHAR2(64)
);

CREATE OR REPLACE VIEW STG.account_balance as
select trunc(the_date,'mm') the_date, account_id, avg(inclusive_balance) inclusive_balance, avg(exact_balance) exact_balance from (
SELECT *
FROM stg.account_balance_daily
) group by trunc(the_date,'mm'), account_id
;

CREATE OR REPLACE VIEW STG.account_balance_daily as
WITH
calendar(the_date) AS (
   SELECT to_date('11.11.2024','dd.mm.yyyy') AS the_date FROM dual  -- исходное множество -- одна строка
   UNION ALL                                                        -- символическое «объединение» строк
   SELECT the_date + 1 AS the_date                                  -- рекурсия: добавок к предыдущему результату
   FROM   calendar                                                  -- предыдущий результат в качестве источника данных
   WHERE  the_date < sysdate                                        -- если не ограничить, будет бесконечная рекурсия
)
--select trunc(the_date,'mm') the_date, account_id, avg(inclusive_balance) inclusive_balance, avg(exact_balance) exact_balance from (
SELECT c.the_date, a.account_id,
nvl((select sum(DECODE(SUBSTR(DEBIT_ACCOUNT_ID,1,length(a.account_id)), a.account_id, amount, 0) - DECODE(SUBSTR(CREDIT_ACCOUNT_ID,1,length(a.account_id)), a.account_id, amount, 0))
    from STG.STG_TRANSACTION
     where (SUBSTR(DEBIT_ACCOUNT_ID,1,length(a.account_id))=a.account_id OR SUBSTR(CREDIT_ACCOUNT_ID,1,length(a.account_id))  = a.account_id)
     and TRANSACTION_DATE <= c.the_date),0) + start_balance as inclusive_balance,
nvl((select sum(DECODE(DEBIT_ACCOUNT_ID, a.account_id, amount, 0)             - DECODE(CREDIT_ACCOUNT_ID, a.account_id, amount, 0))
    from STG.STG_TRANSACTION
     where (DEBIT_ACCOUNT_ID=a.account_id OR CREDIT_ACCOUNT_ID= a.account_id)
     and TRANSACTION_DATE <= c.the_date),0) + start_balance as exact_balance
FROM calendar c
join STG.STG_ACCOUNT a on (1=1)
--) group by trunc(the_date,'mm'), account_id
;
