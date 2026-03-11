-- СОЗДАНИЕ ХАБА СЧЕТОВ
CREATE TABLE RDV2.h_account (
    account_hash_key    VARCHAR2(64) PRIMARY KEY, -- Хэш от бизнес-ключа (account_id)
    account_id          VARCHAR2(16) NOT NULL,    -- Исходный бизнес-ключ
    load_date           DATE NOT NULL,
    record_source       VARCHAR2(50) NOT NULL
);
-- Уникальный индекс на бизнес-ключ
CREATE UNIQUE INDEX RDV2.bk_h_account ON RDV2.h_account (account_id);

-- СОЗДАНИЕ ХАБА ТРАНЗАКЦИЙ
CREATE TABLE RDV2.h_transaction (
    transaction_hash_key    VARCHAR2(64) PRIMARY KEY, -- Хэш от transaction_id
    transaction_id          NUMBER(10) NOT NULL,
    load_date               DATE NOT NULL,
    record_source           VARCHAR2(50) NOT NULL
);
CREATE UNIQUE INDEX RDV2.bk_h_transaction ON RDV2.h_transaction (transaction_id);

SET SERVEROUTPUT ON;
DECLARE
  v_hub_inserted NUMBER(10) := 0;
  v_records_processed NUMBER(10) := 0;
BEGIN
  for X in (
  select LOWER(STANDARD_HASH(t.transaction_id, 'MD5')) as transaction_hash_key,
    t.TRANSACTION_ID,
    SYSDATE load_date,
    'STG_TRANSACTION' record_source
  from STG.STG_TRANSACTION t 
  where not exists 
  (select 1 from RDV2.H_TRANSACTION rt where rt.transaction_id = t.transaction_id))
  LOOP
      insert into RDV2.H_TRANSACTION (transaction_hash_key, transaction_id, load_date, record_source)
      values (x.transaction_hash_key, x.transaction_id, x.load_date, x.record_source);
      v_hub_inserted := v_hub_inserted + 1;
      v_records_processed := v_records_processed + 1;
  END LOOP;
  
  COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
  DBMS_OUTPUT.PUT_LINE('Статистика:');
  DBMS_OUTPUT.PUT_LINE('  - Вставлено в хаб транзакций: ' || v_hub_inserted);
  DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
END;
/

DECLARE
  v_hub_inserted NUMBER(10) := 0;
  v_records_processed NUMBER(10) := 0;
BEGIN
  for X in (
  select LOWER(STANDARD_HASH(t.account_id, 'MD5')) as account_hash_key,
    t.account_id,
    SYSDATE load_date,
    'STG_ACCOUNT' record_source
  from STG.STG_ACCOUNT t 
  where not exists 
  (select 1 from RDV2.H_ACCOUNT rt where rt.account_id = t.account_id))
  LOOP
      insert into RDV2.H_ACCOUNT (account_hash_key, account_id, load_date, record_source)
      values (x.account_hash_key, x.account_id, x.load_date, x.record_source);
      v_hub_inserted := v_hub_inserted + 1;
      v_records_processed := v_records_processed + 1;
  END LOOP;
  
  COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
  DBMS_OUTPUT.PUT_LINE('Статистика:');
  DBMS_OUTPUT.PUT_LINE('  - Вставлено в хаб транзакций: ' || v_hub_inserted);
  DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
END;
/


select * from RDV2.H_TRANSACTION where transaction_id = 3401;
delete from RDV2.H_TRANSACTION where transaction_id > 3390;