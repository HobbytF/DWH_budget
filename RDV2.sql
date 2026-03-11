-- СОЗДАНИЕ ХАБА СЧЕТОВ
CREATE TABLE RDV2.h_account (
    account_hash_key    VARCHAR2(64) PRIMARY KEY, -- Хэш от бизнес-ключа (account_id)
    account_id          VARCHAR2(16) NOT NULL,    -- Исходный бизнес-ключ
    load_date           DATE NOT NULL,
    record_source       VARCHAR2(50) NOT NULL
);
-- Уникальный индекс на бизнес-ключ
CREATE UNIQUE INDEX RDV2.bk_h_account ON RDV2.h_account (account_id);
drop table RDV2.h_transaction;
-- СОЗДАНИЕ ХАБА ТРАНЗАКЦИЙ
CREATE TABLE RDV2.h_transaction (
    transaction_rk    VARCHAR2(64) PRIMARY KEY, -- Хэш от transaction_id
    transaction_id    NUMBER(10) NOT NULL,
    load_date         DATE NOT NULL,
    record_source     VARCHAR2(50) NOT NULL
);
CREATE UNIQUE INDEX RDV2.bk_h_transaction ON RDV2.h_transaction (transaction_id);

-- СОЗДАНИЕ САТТЕЛИТА ДЛЯ ТРАНЗАКЦИЙ
CREATE TABLE RDV2.s_transaction (
    transaction_rk    VARCHAR2(64),         -- Ссылка на хаб транзакций
    valid_from        DATE,                 -- Дата начала действия записи
    valid_to          DATE,                 -- Дата окончания действия записи
    transaction_date  DATE NOT NULL,        -- Дата транзакции
    amount            NUMBER(10,2),         -- Сумма транзакции
    valid_flg         CHAR(1),              -- Флаг валидности
    load_date         DATE,                 -- Дата загрузки записи
    hash_diff         VARCHAR2(64) NOT NULL,-- Хэш по ключам transaction_rk, valid_from, transaction_date, amount
    PRIMARY KEY (transaction_rk, valid_from)
);

select * from rdv2.H_TRANSACTION where TRANSACTION_ID = 3201;
select * from rdv2.S_TRANSACTION where TRANSACTION_RK = '13833976df68c860bdcfc6f9d89fceb8';

DECLARE
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_sat_updated       NUMBER := 0;
    v_sat_inserted      NUMBER := 0;
BEGIN
  DBMS_OUTPUT.PUT_LINE('Начало загрузки сателлита транзакции: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));
  FOR X in (
    select LOWER(STANDARD_HASH(st.TRANSACTION_ID, 'MD5')) as transaction_rk, 
      v_load_timestamp as valid_from, 
      TO_DATE('2999.12.31','yyyy.mm.dd') as valid_to,
      st.TRANSACTION_DATE, 
      st.AMOUNT,
      '1' as valid_flg,
      v_load_timestamp as load_date,
      LOWER(STANDARD_HASH(LOWER(STANDARD_HASH(st.TRANSACTION_ID, 'MD5')) || '|' || st.TRANSACTION_DATE || '|' || st.AMOUNT, 'MD5')) as hash_diff
    from stg.STG_TRANSACTION st 
    where not EXISTS
      (select 1 from RDV2.S_TRANSACTION sat where
      LOWER(STANDARD_HASH(LOWER(STANDARD_HASH(st.TRANSACTION_ID, 'MD5')) || '|' || st.TRANSACTION_DATE || '|' || st.AMOUNT, 'MD5')) = sat.HASH_DIFF
      and sat.valid_flg = '1')
  )
  LOOP
      BEGIN
          DECLARE 
            v_cnt NUMBER(10):=0;
          BEGIN
            -- Пытаемся найти старую запись
            select count(*) into v_cnt from RDV2.S_TRANSACTION sat
            where sat.TRANSACTION_RK = x.transaction_rk and sat.VALID_FLG = '1';
            -- Если старая запись нашлась, то обновляем ее
            IF v_cnt > 0 THEN
              UPDATE RDV2.S_TRANSACTION sat set sat.VALID_FLG = '0', sat.valid_to = v_load_timestamp
              where sat.TRANSACTION_RK = x.transaction_rk and sat.VALID_FLG = '1';
              v_sat_updated := v_sat_updated + 1;
            END IF;
            -- создаем новую версию
            insert into rdv2.S_TRANSACTION (transaction_rk, valid_from, valid_to, transaction_date, amount, VALID_FLG, load_date, hash_diff)
            values(x.transaction_rk, x.valid_from, x.valid_to, x.transaction_date, x.amount, x.VALID_FLG, x.load_date, x.hash_diff);
            v_sat_inserted := v_sat_inserted + 1;
            v_records_processed := v_records_processed + 1;
          END;
      END;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
  DBMS_OUTPUT.PUT_LINE('Статистика:');
  DBMS_OUTPUT.PUT_LINE('  - Обновлено в спутнике: ' || v_sat_updated);
  DBMS_OUTPUT.PUT_LINE('  - Вставлено в спутнике: ' || v_sat_inserted);
  DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
END;
/





SET SERVEROUTPUT ON;
DECLARE
  v_hub_inserted NUMBER(10) := 0;
  v_records_processed NUMBER(10) := 0;
BEGIN
  for X in (
  select LOWER(STANDARD_HASH(t.transaction_id, 'MD5')) as transaction_rk,
    t.TRANSACTION_ID,
    SYSDATE load_date,
    'STG_TRANSACTION' record_source
  from STG.STG_TRANSACTION t 
  where not exists 
  (select 1 from RDV2.H_TRANSACTION rt where rt.transaction_id = t.transaction_id))
  LOOP
      insert into RDV2.H_TRANSACTION (transaction_rk, transaction_id, load_date, record_source)
      values (x.transaction_rk, x.transaction_id, x.load_date, x.record_source);
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
  DBMS_OUTPUT.PUT_LINE('  - Вставлено в хаб счетов: ' || v_hub_inserted);
  DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
END;
/


select * from RDV2.H_TRANSACTION where transaction_id = 3401;
delete from RDV2.H_TRANSACTION where transaction_id > 3390;