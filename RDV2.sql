
-- СОЗДАНИЕ ХАБА СЧЕТОВ
CREATE TABLE RDV2.h_account (
    account_rk          VARCHAR2(64) PRIMARY KEY, -- Хэш от бизнес-ключа (account_id)
    account_id          VARCHAR2(16) NOT NULL,    -- Исходный бизнес-ключ
    load_date           DATE NOT NULL,
    record_source       VARCHAR2(50) NOT NULL
);
-- Уникальный индекс на бизнес-ключ
CREATE UNIQUE INDEX RDV2.bk_h_account ON RDV2.h_account (account_id);

-- СОЗДАНИЕ ХАБА ТРАНЗАКЦИЙ
CREATE TABLE RDV2.h_transaction (
    transaction_rk    VARCHAR2(64) PRIMARY KEY, -- Хэш от transaction_id
    transaction_id    NUMBER(10) NOT NULL,
    load_date         DATE NOT NULL,
    record_source     VARCHAR2(50) NOT NULL
);
CREATE UNIQUE INDEX RDV2.bk_h_transaction ON RDV2.h_transaction (transaction_id);

-- СОЗДАНИЕ САТТЕЛИТА ДЛЯ СЧЕТОВ
CREATE TABLE RDV2.s_account (
    account_rk        VARCHAR2(64),         -- Ссылка на хаб счетов
    valid_from        DATE,                 -- Дата начала действия записи
    valid_to          DATE,                 -- Дата окончания действия записи
    account_name      varchar2(100),        -- Наименование счета
    start_balance     NUMBER(10,2),         -- Начальное сальдо
    valid_flg         CHAR(1),              -- Флаг валидности
    load_date         DATE,                 -- Дата загрузки записи
    hash_diff         VARCHAR2(64) NOT NULL,-- Хэш по ключам account_rk, valid_from, account_name, start_balance
    record_source     VARCHAR2(50),         -- Источник записи
    PRIMARY KEY (account_rk, valid_from)
);

-- СОЗДАНИЕ ЛИНКА ДЛЯ СВЯЗИ ТРАНЗАКЦИИ СО СЧЕТАМИ
CREATE TABLE RDV2.l_transation_account (
    transaction_debacc_credacc_rk   VARCHAR2(64),         -- ХЭШ от связки ключей
    transaction_rk                  VARCHAR2(64),         -- Ссылка на хаб транзакций
    debit_account_rk                VARCHAR2(64),         -- Ссылка на хаб счетов
    credit_account_rk               VARCHAR2(64),         -- Ссылка на хаб счетов
    load_date                       DATE,                 -- Дата загрузки записи
    record_source                   VARCHAR2(50),         -- Источник записи
    PRIMARY KEY (transaction_debacc_credacc_rk)    
);

-- СОЗДАНИЕ САТТЕЛИТА ДЛЯ СВЯЗИ ТРАНЗАКЦИИ СО СЧЕТОМ
CREATE TABLE RDV2.s_transation_account (
    transaction_debacc_credacc_rk   VARCHAR2(64),         -- Ссылка на линк связи транзакций и счетов
    valid_from                      DATE,                 -- Дата начала действия записи
    valid_to                        DATE,                 -- Дата окончания действия записи
    transaction_date                DATE,                 -- Дата транзакции
    amount                          NUMBER(10,2),         -- Сумма транзакции
    valid_flg                       CHAR(1),              -- Флаг валидности
    load_date                       DATE,                 -- Дата загрузки записи
    hash_diff                       VARCHAR2(64) NOT NULL,-- Хэш по ключам transaction_debacc_credacc_rk, valid_from, transaction_date, amount
    record_source                   VARCHAR2(50),         -- Источник записи
    PRIMARY KEY (transaction_debacc_credacc_rk, valid_from)    
);

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
    record_source     VARCHAR2(50),         -- Источник записи
    PRIMARY KEY (transaction_rk, valid_from)
);

drop TABLE RDV2.l_account_account;
-- Связь "Счет-Счет" для иерархии
CREATE TABLE RDV2.l_account_account (
    parent_child_account_rk   VARCHAR2(64) PRIMARY KEY,
    parent_account_rk         VARCHAR2(64) NOT NULL, -- Ссылка на хаб H_ACCOUNT (родитель)
    child_account_rk          VARCHAR2(64) NOT NULL, -- Ссылка на хаб H_ACCOUNT (потомок)
    
    valid_from                DATE,                  -- Дата начала действия записи
    valid_to                  DATE,                  -- Дата окончания действия записи
    valid_flg                 CHAR(1),               -- Флаг валидности
    load_date                 DATE NOT NULL,
    record_source             VARCHAR2(50) NOT NULL
);
CREATE UNIQUE INDEX RDV2.bk_l_acc_acc ON RDV2.l_account_account (parent_account_rk, child_account_rk);

select * from rdv2.H_TRANSACTION where TRANSACTION_ID = 3201;
select * from rdv2.S_TRANSACTION where TRANSACTION_RK = '13833976df68c860bdcfc6f9d89fceb8';

SET SERVEROUTPUT ON;

DECLARE
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_sat_updated       NUMBER := 0;
    v_sat_inserted      NUMBER := 0;
    v_src               VARCHAR2(10) := 'YAST';
    v_days_ago          NUMBER(5) := 30;
BEGIN
  DBMS_OUTPUT.PUT_LINE('Начало загрузки сателлита счета по источнику ' || v_src || ': ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));
  FOR X in (
    select LOWER(STANDARD_HASH(sa.ACCOUNT_ID, 'MD5')) as account_rk, 
      v_load_timestamp as valid_from, 
      TO_DATE('2999.12.31','yyyy.mm.dd') as valid_to,
      sa.ACCOUNT_NAME, 
      sa.START_BALANCE,
      '1' as valid_flg,
      v_load_timestamp as load_date,
      LOWER(STANDARD_HASH(LOWER(STANDARD_HASH(sa.ACCOUNT_ID, 'MD5')) || '|' || sa.ACCOUNT_NAME || '|' || to_char(sa.START_BALANCE, '999999999.99'), 'MD5')) as hash_diff,
      v_src as record_source
    from stg.STG_ACCOUNT sa 
    where not EXISTS
      (select 1 from RDV2.S_ACCOUNT sat where
        LOWER(STANDARD_HASH(LOWER(STANDARD_HASH(sa.ACCOUNT_ID, 'MD5')) || '|' || sa.ACCOUNT_NAME || '|' || to_char(sa.START_BALANCE, '999999999.99'), 'MD5')) = sat.HASH_DIFF
        and sat.valid_flg = '1'  and sat.RECORD_SOURCE = v_src
      )
      and sa.RECORD_SOURCE = v_src
      and sa.LOAD_DATE > TRUNC(sysdate) - v_days_ago
  )
  LOOP
      BEGIN
          DECLARE 
            v_cnt NUMBER(10):=0;
          BEGIN
            -- Пытаемся найти старую запись
            select count(*) into v_cnt from RDV2.S_ACCOUNT sat
            where sat.account_rk = x.account_rk and sat.VALID_FLG = '1' and sat.record_source = x.record_source;
            -- Если старая запись нашлась, то обновляем ее
            IF v_cnt > 0 THEN
              UPDATE RDV2.S_ACCOUNT sat set sat.VALID_FLG = '0', sat.valid_to = v_load_timestamp
              where sat.account_rk = x.account_rk and sat.VALID_FLG = '1' and sat.record_source = x.record_source;
              v_sat_updated := v_sat_updated + 1;
            END IF;
            -- создаем новую версию
            insert into rdv2.S_ACCOUNT (account_rk, valid_from, valid_to, account_name, start_balance, VALID_FLG, load_date, hash_diff, record_source)
            values(x.account_rk, x.valid_from, x.valid_to, x.account_name, x.start_balance, x.VALID_FLG, x.load_date, x.hash_diff, x.record_source);
            v_sat_inserted := v_sat_inserted + 1;
            v_records_processed := v_records_processed + 1;
          END;
      END;
  END LOOP;
  commit;
  DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
  DBMS_OUTPUT.PUT_LINE('Статистика:');
  DBMS_OUTPUT.PUT_LINE('  - Обновлено в спутнике: ' || v_sat_updated);
  DBMS_OUTPUT.PUT_LINE('  - Вставлено в спутнике: ' || v_sat_inserted);
  DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
END;
/

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
      LOWER(STANDARD_HASH(LOWER(STANDARD_HASH(st.TRANSACTION_ID, 'MD5')) || '|' || st.TRANSACTION_DATE || '|' || st.AMOUNT, 'MD5')) as hash_diff,
      'STG_TRANSACTION' as record_source
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
            insert into rdv2.S_TRANSACTION (transaction_rk, valid_from, valid_to, transaction_date, amount, VALID_FLG, load_date, hash_diff, record_source)
            values(x.transaction_rk, x.valid_from, x.valid_to, x.transaction_date, x.amount, x.VALID_FLG, x.load_date, x.hash_diff, x.record_source);
            v_sat_inserted := v_sat_inserted + 1;
            v_records_processed := v_records_processed + 1;
          END;
      END;
  END LOOP;
  commit;
  DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
  DBMS_OUTPUT.PUT_LINE('Статистика:');
  DBMS_OUTPUT.PUT_LINE('  - Обновлено в спутнике: ' || v_sat_updated);
  DBMS_OUTPUT.PUT_LINE('  - Вставлено в спутнике: ' || v_sat_inserted);
  DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
END;
/






DECLARE
  v_load_timestamp    DATE := SYSDATE;
  v_hub_inserted NUMBER(10) := 0;
  v_records_processed NUMBER(10) := 0;
  v_src               VARCHAR2(10) := 'YAST';
BEGIN
  DBMS_OUTPUT.PUT_LINE('Начало загрузки хаба транзакции: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));
  for X in (
  select LOWER(STANDARD_HASH(t.transaction_id, 'MD5')) as transaction_rk,
    t.TRANSACTION_ID,
    v_load_timestamp load_date,
    v_src record_source
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
  v_load_timestamp    DATE := SYSDATE;
  v_hub_inserted NUMBER(10) := 0;
  v_records_processed NUMBER(10) := 0;
  v_src               VARCHAR2(10) := 'YAST';
BEGIN
  DBMS_OUTPUT.PUT_LINE('Начало загрузки хаба счетов: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));
  for X in (
  select LOWER(STANDARD_HASH(t.account_id, 'MD5')) as account_rk,
    t.account_id,
    v_load_timestamp load_date,
    v_src record_source
  from STG.STG_ACCOUNT t 
  where not exists 
  (select 1 from RDV2.H_ACCOUNT rt where rt.account_id = t.account_id))
  LOOP
      insert into RDV2.H_ACCOUNT (account_rk, account_id, load_date, record_source)
      values (x.account_rk, x.account_id, x.load_date, x.record_source);
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


DECLARE
    v_load_timestamp    DATE := SYSDATE;
    v_lnk_updated       NUMBER := 0;
    v_lnk_inserted      NUMBER := 0;
    v_records_processed NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало связей счет-счет для иерархии: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));
    -- Вставляем связи "родитель-потомок" на основе структуры account_id
    FOR rec IN (
        select 
            LOWER(STANDARD_HASH(LOWER(STANDARD_HASH(SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)), 'MD5')) || '|' || LOWER(STANDARD_HASH(account_id, 'MD5')), 'MD5')) as parent_child_account_rk,
            SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)) parent_account_id, 
            account_id child_account_id,
            LOWER(STANDARD_HASH(SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)), 'MD5')) as parent_account_rk,
            LOWER(STANDARD_HASH(account_id, 'MD5')) as child_account_rk,
            v_load_timestamp as valid_from,
            TO_DATE('2999.12.31','yyyy.mm.dd') as valid_to,
            '1' as valid_flg,
            v_load_timestamp as load_date,
            'YAST' as record_source
        from stg.stg_account
        where SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)) is not null
        and not EXISTS (
          select 1 from rdv2.l_account_account laa 
          WHERE laa.parent_child_account_rk = LOWER(STANDARD_HASH(LOWER(STANDARD_HASH(SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)), 'MD5')) || '|' || LOWER(STANDARD_HASH(account_id, 'MD5')), 'MD5'))
          AND laa.valid_flg = '1'
        )
    ) 
    LOOP
      BEGIN
          DECLARE 
            v_cnt NUMBER(10):=0;
          BEGIN
            -- Пытаемся найти старую запись
            select count(*) into v_cnt from RDV2.L_ACCOUNT_ACCOUNT laa
            where laa.parent_child_account_rk = rec.parent_child_account_rk and laa.VALID_FLG = '1';
            -- Если старая запись нашлась, то обновляем ее
            IF v_cnt > 0 THEN
              UPDATE RDV2.L_ACCOUNT_ACCOUNT laa set laa.VALID_FLG = '0', laa.valid_to = v_load_timestamp
              where laa.parent_child_account_rk = rec.parent_child_account_rk and laa.VALID_FLG = '1';
              v_lnk_updated := v_lnk_updated + 1;
            END IF;
            -- создаем новую версию
            insert into rdv2.L_ACCOUNT_ACCOUNT (parent_child_account_rk, parent_account_rk, child_account_rk, valid_from, valid_to, valid_flg, load_date, record_source)
            values(rec.parent_child_account_rk, rec.parent_account_rk, rec.child_account_rk, rec.valid_from, rec.valid_to, rec.valid_flg, rec.load_date, rec.record_source);
            v_lnk_inserted := v_lnk_inserted + 1;
            v_records_processed := v_records_processed + 1;
          END;
      END;
    END LOOP;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
    DBMS_OUTPUT.PUT_LINE('Статистика:');
    DBMS_OUTPUT.PUT_LINE('  - Обновлено в линке иерархии: ' || v_lnk_updated);
    DBMS_OUTPUT.PUT_LINE('  - Вставлено в линк иерархии: ' || v_lnk_inserted);
    DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Ошибка при загрузке иерархии: ' || SQLERRM);
        RAISE;
END;
/

select * from RDV2.H_TRANSACTION where transaction_id = 3401;
delete from RDV2.H_TRANSACTION where transaction_id > 3390;