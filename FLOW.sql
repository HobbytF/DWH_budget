-- Таблица для логирования ошибок
CREATE TABLE FLOW.load_error_log (
    error_id        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    error_date      DATE DEFAULT SYSDATE,
    procedure_name  VARCHAR2(100),
    error_message   VARCHAR2(4000),
    account_id      VARCHAR2(16),
    transaction_id  NUMBER(10),
    record_data     CLOB
);

-- Загрузка хаба счетов
CREATE OR REPLACE PROCEDURE flow.wrk_stg_rdv_hub_account 
IS
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
  
  DBMS_OUTPUT.PUT_LINE('Загрузка хаба счетов завершена успешно.');
  DBMS_OUTPUT.PUT_LINE('Статистика:');
  DBMS_OUTPUT.PUT_LINE('  - Вставлено в хаб счетов: ' || v_hub_inserted);
  DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
END wrk_stg_rdv_hub_account;
/

-- Загрузка хаба транзакции
CREATE OR REPLACE PROCEDURE flow.wrk_stg_rdv_hub_transaction
IS
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
END wrk_stg_rdv_hub_transaction;
/

-- Загрузка сателлита счета по источнику
CREATE OR REPLACE PROCEDURE flow.wrk_stg_rdv_sat_account (v_src in varchar2, v_days_ago in number)
IS
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_sat_updated       NUMBER := 0;
    v_sat_inserted      NUMBER := 0;
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
END wrk_stg_rdv_sat_account;
/

-- Загрузка связей счет-счет для иерархии (SCD0)
CREATE OR REPLACE PROCEDURE flow.wrk_stg_rdv_lnk_account_account
IS
    v_load_timestamp    DATE := SYSDATE;
    v_lnk_updated       NUMBER := 0;
    v_lnk_inserted      NUMBER := 0;
    v_records_processed NUMBER := 0;
    v_src               VARCHAR2(50):='YAST';
BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки связей счет-счет для иерархии: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));
    -- Вставляем связи "родитель-потомок" на основе структуры account_id
    FOR rec IN (
      select 
          LOWER(STANDARD_HASH(LOWER(STANDARD_HASH(SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)), 'MD5')) || '|' || LOWER(STANDARD_HASH(account_id, 'MD5')), 'MD5')) as parent_child_account_rk,
          LOWER(STANDARD_HASH(SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)), 'MD5')) as parent_account_rk,
          LOWER(STANDARD_HASH(account_id, 'MD5')) as child_account_rk,
          v_load_timestamp as load_date,
          v_src as record_source
      from stg.stg_account
      where SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)) is not null
      and not EXISTS (
        select 1 from rdv2.l_account_account laa 
        WHERE laa.parent_child_account_rk = LOWER(STANDARD_HASH(LOWER(STANDARD_HASH(SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)), 'MD5')) || '|' || LOWER(STANDARD_HASH(account_id, 'MD5')), 'MD5'))
      )
    ) 
    LOOP
      BEGIN
          DECLARE 
            v_cnt NUMBER(10):=0;
          BEGIN
            -- Пытаемся найти неактуальную запись
            select count(*) into v_cnt from RDV2.L_ACCOUNT_ACCOUNT laa
            where laa.child_account_rk = rec.child_account_rk;
            -- Если такая запись нашлась, то обновляем ее
            IF v_cnt > 0 THEN
              begin
                UPDATE RDV2.L_ACCOUNT_ACCOUNT laa set laa.load_date = v_load_timestamp, laa.parent_child_account_rk = rec.parent_child_account_rk, laa.parent_account_rk = rec.parent_account_rk
                where laa.child_account_rk = rec.child_account_rk;
                v_lnk_updated := v_lnk_updated + 1;
              end;
            ELSE
              begin
                -- создаем новую версию
                insert into rdv2.L_ACCOUNT_ACCOUNT (parent_child_account_rk, parent_account_rk, child_account_rk, load_date, record_source)
                values(rec.parent_child_account_rk, rec.parent_account_rk, rec.child_account_rk, rec.load_date, rec.record_source);
                v_lnk_inserted := v_lnk_inserted + 1;
              end;
            END IF;
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

-- Загрузка связи транзакции и счетов, саттелита связи
CREATE OR REPLACE PROCEDURE flow.wrk_stg_rdv_lnk_transaction_account (v_src in varchar2, v_days_ago in number)
IS
    v_load_timestamp    DATE := SYSDATE;
    v_sat_updated       NUMBER := 0;
    v_sat_inserted      NUMBER := 0;
    v_lnk_inserted      NUMBER := 0;
    v_records_processed NUMBER := 0;
    v_dummy_detected    BOOLEAN := FALSE;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки связи транзакции и счетов, саттелита связи  по источнику ' || v_src || ': ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));
    
    FOR rec IN (
      SELECT 
      xta.* ,
      LOWER(STANDARD_HASH(xta.TRANSACTION_DEBACC_CREDACC_RK || '|' || to_char(xta.TRANSACTION_DATE, 'dd.mm.yyyy HH24:mi:ss') || '|' || to_char(xta.AMOUNT, '999999999.99'), 'MD5')) as hash_diff
      FROM (
        SELECT 
          LOWER(STANDARD_HASH(lta.TRANSACTION_RK || '|' || lta.DEBIT_ACCOUNT_RK || '|' || lta.CREDIT_ACCOUNT_RK , 'MD5')) AS TRANSACTION_DEBACC_CREDACC_RK,
          lta.* ,
          v_load_timestamp AS LOAD_DATE ,
          v_src AS RECORD_SOURCE
        FROM (
            SELECT 
            (SELECT ht.TRANSACTION_RK FROM rdv2.H_TRANSACTION ht WHERE ht.TRANSACTION_ID  = t.TRANSACTION_ID) AS TRANSACTION_RK ,
            (SELECT had.ACCOUNT_RK FROM rdv2.H_ACCOUNT had WHERE had.ACCOUNT_ID = t.DEBIT_ACCOUNT_ID) AS DEBIT_ACCOUNT_RK ,
            (SELECT hac.ACCOUNT_RK FROM rdv2.H_ACCOUNT hac WHERE hac.ACCOUNT_ID = t.CREDIT_ACCOUNT_ID) AS CREDIT_ACCOUNT_RK ,
            t.TRANSACTION_DATE ,
            t.AMOUNT 
          FROM  stg.stg_transaction t
          WHERE RECORD_SOURCE = v_src
          AND LOAD_DATE > SYSDATE - v_days_ago
        ) lta
      ) xta
      WHERE NOT EXISTS 
        (SELECT 1 FROM RDV2.L_TRANSACTION_ACCOUNT lta WHERE lta.TRANSACTION_DEBACC_CREDACC_RK  = xta.TRANSACTION_DEBACC_CREDACC_RK)
        OR NOT EXISTS
        (SELECT 1 FROM RDV2.S_TRANSACTION_ACCOUNT sta 
          WHERE 	
            sta.HASH_DIFF = LOWER(STANDARD_HASH(xta.TRANSACTION_DEBACC_CREDACC_RK || '|' || to_char(xta.TRANSACTION_DATE, 'dd.mm.yyyy HH24:mi:ss') || '|' || to_char(xta.AMOUNT, '999999999.99'), 'MD5'))
            AND sta.VALID_FLG = '1'
        )
    ) 
    LOOP
      BEGIN
          DECLARE 
            v_cnt NUMBER(10):=0;
            v_lta_rk varchar2(64):='';
          BEGIN
            IF (rec.TRANSACTION_RK is null or rec.DEBIT_ACCOUNT_RK is null or rec.CREDIT_ACCOUNT_RK is null) THEN -- Если хотя бы один ключ не нашелся в хабе, то не грузим
              v_dummy_detected := TRUE;
            ELSE       -- Если все ключи найдены, то грузим
              -- Смотрим, есть ли запись связи 
              select count(*) into v_cnt from RDV2.L_TRANSACTION_ACCOUNT lta
              where lta.TRANSACTION_DEBACC_CREDACC_RK = rec.TRANSACTION_DEBACC_CREDACC_RK;
              IF v_cnt > 0 then -- Если находим линк, то обновляем информацию по нему
                BEGIN
                    select lta.TRANSACTION_DEBACC_CREDACC_RK into v_lta_rk from RDV2.L_TRANSACTION_ACCOUNT lta 
                    join RDV2.S_TRANSACTION_ACCOUNT sta on (sta.TRANSACTION_DEBACC_CREDACC_RK = lta.TRANSACTION_DEBACC_CREDACC_RK and sta.valid_flg = '1')
                    where lta.transaction_rk = rec.TRANSACTION_RK;
                    update RDV2.S_TRANSACTION_ACCOUNT sta set sta.valid_flg = '0', sta.valid_to = v_load_timestamp 
                    where sta.TRANSACTION_DEBACC_CREDACC_RK = v_lta_rk;
                    v_sat_updated := v_sat_updated + 1;
                EXCEPTION
                  when NO_DATA_FOUND then 
                    update RDV2.S_TRANSACTION_ACCOUNT sta set sta.valid_flg = '0', sta.valid_to = v_load_timestamp 
                    where sta.TRANSACTION_DEBACC_CREDACC_RK = rec.TRANSACTION_DEBACC_CREDACC_RK;
                    v_sat_updated := v_sat_updated + 1;
                END;

              ELSE
                -- Если линка нет, то находим старый линк по хэшу транзакции
                BEGIN
                    select lta.TRANSACTION_DEBACC_CREDACC_RK into v_lta_rk from RDV2.L_TRANSACTION_ACCOUNT lta 
                    join RDV2.S_TRANSACTION_ACCOUNT sta on (sta.TRANSACTION_DEBACC_CREDACC_RK = lta.TRANSACTION_DEBACC_CREDACC_RK and sta.valid_flg = '1')
                    where lta.transaction_rk = rec.TRANSACTION_RK;
                    update RDV2.S_TRANSACTION_ACCOUNT sta set sta.valid_flg = '0', sta.valid_to = v_load_timestamp 
                    where sta.TRANSACTION_DEBACC_CREDACC_RK = v_lta_rk;
                    v_sat_updated := v_sat_updated + 1;
                EXCEPTION
                  when NO_DATA_FOUND then v_lta_rk :='none';
                END;
                insert into RDV2.L_TRANSACTION_ACCOUNT
                (transaction_debacc_credacc_rk, transaction_rk, debit_account_rk, credit_account_rk, load_date, record_source)
                values (rec.transaction_debacc_credacc_rk, rec.transaction_rk, rec.debit_account_rk, rec.credit_account_rk, rec.load_date, rec.record_source);
                v_lnk_inserted := v_lnk_inserted + 1;
              END IF;
              insert into RDV2.S_TRANSACTION_ACCOUNT 
              (transaction_debacc_credacc_rk, valid_from, valid_to, transaction_date, amount, valid_flg, load_date, hash_diff, record_source)
              values (rec.transaction_debacc_credacc_rk, v_load_timestamp, TO_DATE('2999.12.31','yyyy.mm.dd'), rec.transaction_date, rec.amount, '1', v_load_timestamp, rec.hash_diff, v_src);
              
              v_sat_inserted := v_sat_inserted + 1;
              v_records_processed := v_records_processed + 1;
            END IF;
          END;
      END;
    END LOOP;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
    DBMS_OUTPUT.PUT_LINE('Статистика:');
    IF v_dummy_detected then DBMS_OUTPUT.PUT_LINE('Обнаружены dummy-записи. Требуется прогрузка хабов'); END IF;
    DBMS_OUTPUT.PUT_LINE('  - Вставлено в линк: ' || v_lnk_inserted);
    DBMS_OUTPUT.PUT_LINE('  - Вставлено в саттелит: ' || v_sat_inserted);
    DBMS_OUTPUT.PUT_LINE('  - Обновлено в саттелите: ' || v_sat_updated);
    DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Ошибка при загрузке иерархии: ' || SQLERRM);
        RAISE;
END wrk_stg_rdv_lnk_transaction_account;
/

CREATE OR REPLACE PROCEDURE flow.cf_stg_rdv_complete_load (v_src in varchar2, v_days_ago in number)
IS
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := SYSTIMESTAMP;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Запуск полной загрузки Data Vault');
    DBMS_OUTPUT.PUT_LINE('Время начала: ' || TO_CHAR(v_start_time, 'DD.MM.YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Шаг 1: Загрузка счетов
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '1. Загрузка счетов...');
    flow.wrk_stg_rdv_hub_account;
    
    -- Шаг 2: Загрузка иерархии счетов
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '2. Загрузка иерархии счетов...');
    flow.wrk_stg_rdv_lnk_account_account;
    
    -- Шаг 3: Загрузка сателлита счетов
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '3. Загрузка сателлита счетов...');
    flow.wrk_stg_rdv_sat_account (v_src, v_days_ago);

    -- Шаг 4: Загрузка транзакций
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '4. Загрузка транзакций...');
    flow.wrk_stg_rdv_hub_transaction;
    
    -- Шаг 5: Загрузка связей счет-транзакция и их саттелитов
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '5. Загрузка связей счет-транзакция и их саттелитов...');
    flow.wrk_stg_rdv_lnk_transaction_account (v_src, v_days_ago);

    
    -- Шаг 6: Загрузка PIT-таблицы
--    DBMS_OUTPUT.PUT_LINE(CHR(10) || '6. Загрузка PIT-таблицы для счетов...');
--    flow.load_pit_account(TRUNC(SYSDATE));
    
    v_end_time := SYSTIMESTAMP;
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '========================================');
    DBMS_OUTPUT.PUT_LINE('Загрузка Raw Data Vault завершена');
    DBMS_OUTPUT.PUT_LINE('Время окончания: ' || TO_CHAR(v_end_time, 'DD.MM.YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Общее время выполнения: ' || 
                         EXTRACT(MINUTE FROM (v_end_time - v_start_time)) || ' мин. ' ||
                         EXTRACT(SECOND FROM (v_end_time - v_start_time)) || ' сек.');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || 'ОШИБКА при выполнении загрузки: ' || SQLERRM);
        RAISE;
END cf_stg_rdv_complete_load;
/

-- 1. Исправленная процедура загрузки бриджа для связи счетов с транзакциями
CREATE OR REPLACE PROCEDURE flow.load_bridge_account_transaction
IS
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_error_message     VARCHAR2(4000);
    v_sqlcode           NUMBER;
    v_sqlerrm           VARCHAR2(4000);
    
    -- Курсор для получения данных с учетом версионности
    CURSOR c_transaction_data IS
        SELECT 
            ht.transaction_id,
            sat.transaction_date,
            sat.amount,
            ha_debit.account_id as debit_account_id,
            sa_debit.account_name as debit_account_name,
            sa_debit.start_balance as debit_start_balance,
            ha_parent_debit.account_id as debit_parent_account_id,
            ha_credit.account_id as credit_account_id,
            sa_credit.account_name as credit_account_name,
            sa_credit.start_balance as credit_start_balance,
            ha_parent_credit.account_id as credit_parent_account_id,
            LOWER(STANDARD_HASH(
                ht.transaction_id || '|' || 
                ha_debit.account_id || '|' || 
                ha_credit.account_id || '|' ||
                TO_CHAR(sat.transaction_date, 'YYYYMMDD'),
                'MD5'
            )) as hash_key
        FROM rdv.l_account_transaction lat
        JOIN rdv.h_transaction ht ON lat.transaction_hash_key = ht.transaction_hash_key
        JOIN rdv.h_account ha_debit ON lat.debit_account_hash_key = ha_debit.account_hash_key
        JOIN rdv.h_account ha_credit ON lat.credit_account_hash_key = ha_credit.account_hash_key
        -- Только актуальные версии спутников
        JOIN rdv.s_account_transaction sat 
            ON lat.account_txn_hash_key = sat.account_txn_hash_key 
            AND sat.load_end_date IS NULL
        JOIN rdv.s_account sa_debit 
            ON ha_debit.account_hash_key = sa_debit.account_hash_key 
            AND sa_debit.load_end_date IS NULL
        JOIN rdv.s_account sa_credit 
            ON ha_credit.account_hash_key = sa_credit.account_hash_key 
            AND sa_credit.load_end_date IS NULL
        -- Родительские счета (через иерархию)
        LEFT JOIN rdv.l_account_account laa_debit 
            ON ha_debit.account_hash_key = laa_debit.child_account_hash_key
        LEFT JOIN rdv.h_account ha_parent_debit 
            ON laa_debit.parent_account_hash_key = ha_parent_debit.account_hash_key
        LEFT JOIN rdv.l_account_account laa_credit 
            ON ha_credit.account_hash_key = laa_credit.child_account_hash_key
        LEFT JOIN rdv.h_account ha_parent_credit 
            ON laa_credit.parent_account_hash_key = ha_parent_credit.account_hash_key
        WHERE sat.transaction_date IS NOT NULL;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки бриджа счет-транзакция: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));

    BEGIN
        -- Удаляем старые записи для этой даты загрузки
        DELETE FROM idl.bridge_account_transaction 
        WHERE load_date = TRUNC(v_load_timestamp);
        
        v_records_processed := SQL%ROWCOUNT;
        IF v_records_processed > 0 THEN
            DBMS_OUTPUT.PUT_LINE('Удалено старых записей: ' || v_records_processed);
        END IF;

        -- Вставляем новые данные
        FOR rec IN c_transaction_data LOOP
            BEGIN
                INSERT INTO idl.bridge_account_transaction (
                    transaction_id,
                    transaction_date,
                    amount,
                    debit_account_id,
                    debit_account_name,
                    debit_start_balance,
                    debit_parent_account_id,
                    credit_account_id,
                    credit_account_name,
                    credit_start_balance,
                    credit_parent_account_id,
                    load_date,
                    hash_key
                ) VALUES (
                    rec.transaction_id,
                    rec.transaction_date,
                    rec.amount,
                    rec.debit_account_id,
                    rec.debit_account_name,
                    rec.debit_start_balance,
                    rec.debit_parent_account_id,
                    rec.credit_account_id,
                    rec.credit_account_name,
                    rec.credit_start_balance,
                    rec.credit_parent_account_id,
                    v_load_timestamp,
                    rec.hash_key
                );
                
                v_records_processed := v_records_processed + 1;
                
                -- Логирование прогресса
                IF MOD(v_records_processed, 1000) = 0 THEN
                    DBMS_OUTPUT.PUT_LINE('Обработано записей: ' || v_records_processed);
                END IF;
                
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    NULL; -- Игнорируем дубликаты
                WHEN OTHERS THEN
                    v_error_message := 'Ошибка при обработке transaction_id=' || rec.transaction_id || ': ' || SQLERRM;
                    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                    
                    INSERT INTO flow.load_error_log (
                        procedure_name,
                        error_message,
                        transaction_id,
                        record_data
                    ) VALUES (
                        'LOAD_BRIDGE_ACCOUNT_TRANSACTION',
                        v_error_message,
                        rec.transaction_id,
                        'debit_account=' || rec.debit_account_id || ', credit_account=' || rec.credit_account_id
                    );
            END;
        END LOOP;
        
        COMMIT;
        
        -- Обновляем таблицу отслеживания
        UPDATE idl.load_tracking 
        SET last_load_date = v_load_timestamp,
            records_processed = v_records_processed,
            load_status = 'COMPLETED',
            error_message = NULL
        WHERE table_name = 'BRIDGE_ACCOUNT_TRANSACTION';
        
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Загрузка бриджа счет-транзакция завершена успешно.');
        DBMS_OUTPUT.PUT_LINE('Всего загружено записей: ' || v_records_processed);
        
    EXCEPTION
        WHEN OTHERS THEN
            v_sqlcode := SQLCODE;
            v_sqlerrm := SQLERRM;
            ROLLBACK;
            
            -- Обновляем статус ошибки
            UPDATE idl.load_tracking 
            SET load_status = 'ERROR',
                error_message = v_sqlerrm
            WHERE table_name = 'BRIDGE_ACCOUNT_TRANSACTION';
            COMMIT;
            
            v_error_message := 'Критическая ошибка при загрузке бриджа счет-транзакция: ' || v_sqlerrm;
            DBMS_OUTPUT.PUT_LINE('Критическая ошибка: ' || v_error_message);
            RAISE;
    END;
    
END load_bridge_account_transaction;
/

-- 2. Исправленная процедура загрузки бриджа для иерархии счетов
CREATE OR REPLACE PROCEDURE flow.load_bridge_account_hierarchy
IS
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_error_message     VARCHAR2(4000);
    v_sqlcode           NUMBER;
    v_sqlerrm           VARCHAR2(4000);
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки бриджа иерархии счетов: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));

    BEGIN
        -- Очищаем таблицу перед загрузкой
        DELETE FROM idl.bridge_account_hierarchy;
        v_records_processed := SQL%ROWCOUNT;
        
        IF v_records_processed > 0 THEN
            DBMS_OUTPUT.PUT_LINE('Очищено записей: ' || v_records_processed);
        END IF;

        -- Используем INSERT с подзапросом вместо курсора для рекурсивного запроса
        INSERT INTO idl.bridge_account_hierarchy (
            account_id,
            account_name,
            start_balance,
            parent_account_id,
            parent_account_name,
            level_number,
            path_string,
            load_date,
            hash_key
        )
        WITH account_hierarchy (
            account_id,
            account_name,
            start_balance,
            parent_account_id,
            parent_account_name,
            level_number,
            path_string,
            hash_key
        ) AS (
            -- Корневые узлы (без родителей)
            SELECT 
                ha.account_id,
                sa.account_name,
                sa.start_balance,
                NULL as parent_account_id,
                NULL as parent_account_name,
                1 as level_number,
                '/' || sa.account_name as path_string,
                LOWER(STANDARD_HASH(ha.account_id || '|ROOT', 'MD5')) as hash_key
            FROM rdv.h_account ha
            JOIN rdv.s_account sa ON ha.account_hash_key = sa.account_hash_key 
                AND sa.load_end_date IS NULL
            WHERE NOT EXISTS (
                SELECT 1 FROM rdv.l_account_account laa 
                WHERE laa.child_account_hash_key = ha.account_hash_key
            )
            
            UNION ALL
            
            -- Дочерние узлы
            SELECT 
                ha_child.account_id,
                sa_child.account_name,
                sa_child.start_balance,
                ah.account_id as parent_account_id,
                ah.account_name as parent_account_name,
                ah.level_number + 1 as level_number,
                ah.path_string || '/' || sa_child.account_name as path_string,
                LOWER(STANDARD_HASH(ha_child.account_id || '|' || ah.account_id, 'MD5')) as hash_key
            FROM account_hierarchy ah
            JOIN rdv.l_account_account laa ON LOWER(STANDARD_HASH(ah.account_id, 'MD5')) = laa.parent_account_hash_key
            JOIN rdv.h_account ha_child ON laa.child_account_hash_key = ha_child.account_hash_key
            JOIN rdv.s_account sa_child ON ha_child.account_hash_key = sa_child.account_hash_key AND sa_child.load_end_date IS NULL
/*            JOIN rdv.h_account ha_parent ON ah.account_id = ha_parent.account_id
            JOIN rdv.s_account sa_parent ON ha_parent.account_hash_key = sa_parent.account_hash_key AND sa_parent.load_end_date IS NULL*/
        )
        SELECT 
            account_id,
            account_name,
            start_balance,
            parent_account_id,
            parent_account_name,
            level_number,
            path_string,
            v_load_timestamp,
            hash_key
        FROM account_hierarchy;
        
        v_records_processed := SQL%ROWCOUNT;
        COMMIT;
        
        -- Обновляем таблицу отслеживания
        UPDATE idl.load_tracking 
        SET last_load_date = v_load_timestamp,
            records_processed = v_records_processed,
            load_status = 'COMPLETED',
            error_message = NULL
        WHERE table_name = 'BRIDGE_ACCOUNT_HIERARCHY';
        
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Загрузка бриджа иерархии счетов завершена успешно.');
        DBMS_OUTPUT.PUT_LINE('Всего загружено записей: ' || v_records_processed);
        
    EXCEPTION
        WHEN OTHERS THEN
            v_sqlcode := SQLCODE;
            v_sqlerrm := SQLERRM;
            ROLLBACK;
            
            UPDATE idl.load_tracking 
            SET load_status = 'ERROR',
                error_message = v_sqlerrm
            WHERE table_name = 'BRIDGE_ACCOUNT_HIERARCHY';
            COMMIT;
            
            v_error_message := 'Критическая ошибка при загрузке бриджа иерархии: ' || v_sqlerrm;
            DBMS_OUTPUT.PUT_LINE('Критическая ошибка: ' || v_error_message);
            RAISE;
    END;
    
END load_bridge_account_hierarchy;
/

-- 3. Исправленная процедура загрузки бриджа агрегированных данных по счетам
CREATE OR REPLACE PROCEDURE flow.load_bridge_account_aggregation
IS
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_error_message     VARCHAR2(4000);
    v_sqlcode           NUMBER;
    v_sqlerrm           VARCHAR2(4000);
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки бриджа агрегированных данных: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));

    BEGIN
        -- Очищаем таблицу перед загрузкой
        DELETE FROM idl.bridge_account_aggregation;
        v_records_processed := SQL%ROWCOUNT;
        
        IF v_records_processed > 0 THEN
            DBMS_OUTPUT.PUT_LINE('Очищено записей: ' || v_records_processed);
        END IF;

        -- Вставляем агрегированные данные
        INSERT INTO idl.bridge_account_aggregation (
            account_id,
            account_name,
            start_balance,
            total_debit_amount,
            total_credit_amount,
            transaction_count,
            last_transaction_date,
            load_date,
            hash_key
        )
        SELECT 
            ha.account_id,
            sa.account_name,
            sa.start_balance,
            NVL(debit_sums.total_debit, 0) as total_debit_amount,
            NVL(credit_sums.total_credit, 0) as total_credit_amount,
            NVL(txn_counts.txn_count, 0) as transaction_count,
            last_txn.last_transaction_date,
            v_load_timestamp,
            LOWER(STANDARD_HASH(ha.account_id || '|AGG|' || TO_CHAR(v_load_timestamp, 'YYYYMMDD'), 'MD5')) as hash_key
        FROM rdv.h_account ha
        JOIN rdv.s_account sa ON ha.account_hash_key = sa.account_hash_key 
            AND sa.load_end_date IS NULL
        LEFT JOIN (
            SELECT 
                debit_account_id as account_id,
                SUM(amount) as total_debit
            FROM idl.bridge_account_transaction bat
            WHERE bat.load_date = TRUNC(v_load_timestamp)
            GROUP BY debit_account_id
        ) debit_sums ON ha.account_id = debit_sums.account_id
        LEFT JOIN (
            SELECT 
                credit_account_id as account_id,
                SUM(amount) as total_credit
            FROM idl.bridge_account_transaction bat
--            WHERE bat.load_date = TRUNC(v_load_timestamp)
            GROUP BY credit_account_id
        ) credit_sums ON ha.account_id = credit_sums.account_id
        LEFT JOIN (
            SELECT 
                account_id,
                COUNT(*) as txn_count
            FROM (
                SELECT debit_account_id as account_id FROM idl.bridge_account_transaction --WHERE load_date = TRUNC(v_load_timestamp)
                UNION ALL
                SELECT credit_account_id as account_id FROM idl.bridge_account_transaction --WHERE load_date = TRUNC(v_load_timestamp)
            )
            GROUP BY account_id
        ) txn_counts ON ha.account_id = txn_counts.account_id
        LEFT JOIN (
            SELECT 
                account_id,
                MAX(transaction_date) as last_transaction_date
            FROM (
                SELECT 
                    debit_account_id as account_id,
                    transaction_date
                FROM idl.bridge_account_transaction 
--                WHERE load_date = TRUNC(v_load_timestamp)
                UNION ALL
                SELECT 
                    credit_account_id as account_id,
                    transaction_date
                FROM idl.bridge_account_transaction 
 --               WHERE load_date = TRUNC(v_load_timestamp)
            )
            GROUP BY account_id
        ) last_txn ON ha.account_id = last_txn.account_id;
        
        v_records_processed := SQL%ROWCOUNT;
        COMMIT;
        
        -- Обновляем таблицу отслеживания
        UPDATE idl.load_tracking 
        SET last_load_date = v_load_timestamp,
            records_processed = v_records_processed,
            load_status = 'COMPLETED',
            error_message = NULL
        WHERE table_name = 'BRIDGE_ACCOUNT_AGGREGATION';
        
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Загрузка бриджа агрегированных данных завершена успешно.');
        DBMS_OUTPUT.PUT_LINE('Всего загружено записей: ' || v_records_processed);
        
    EXCEPTION
        WHEN OTHERS THEN
            v_sqlcode := SQLCODE;
            v_sqlerrm := SQLERRM;
            ROLLBACK;
            
            UPDATE idl.load_tracking 
            SET load_status = 'ERROR',
                error_message = v_sqlerrm
            WHERE table_name = 'BRIDGE_ACCOUNT_AGGREGATION';
            COMMIT;
            
            v_error_message := 'Критическая ошибка при загрузке агрегированных данных: ' || v_sqlerrm;
            DBMS_OUTPUT.PUT_LINE('Критическая ошибка: ' || v_error_message);
            RAISE;
    END;
    
END load_bridge_account_aggregation;
/

-- 4. Исправленная процедура для полной загрузки IDL слоя
CREATE OR REPLACE PROCEDURE flow.run_complete_idl_load
IS
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := SYSTIMESTAMP;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Запуск полной загрузки IDL слоя');
    DBMS_OUTPUT.PUT_LINE('Время начала: ' || TO_CHAR(v_start_time, 'DD.MM.YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Шаг 1: Загрузка бриджа счет-транзакция
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '1. Загрузка бриджа счет-транзакция...');
    flow.load_bridge_account_transaction;
    
    -- Шаг 2: Загрузка бриджа иерархии счетов
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '2. Загрузка бриджа иерархии счетов...');
    flow.load_bridge_account_hierarchy;
    
    -- Шаг 3: Загрузка бриджа агрегированных данных
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '3. Загрузка бриджа агрегированных данных...');
    flow.load_bridge_account_aggregation;
    
    v_end_time := SYSTIMESTAMP;
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '========================================');
    DBMS_OUTPUT.PUT_LINE('Загрузка IDL слоя завершена');
    DBMS_OUTPUT.PUT_LINE('Время окончания: ' || TO_CHAR(v_end_time, 'DD.MM.YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Общее время выполнения: ' || 
                         EXTRACT(MINUTE FROM (v_end_time - v_start_time)) || ' мин. ' ||
                         EXTRACT(SECOND FROM (v_end_time - v_start_time)) || ' сек.');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Проверка статусов загрузки
    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Статусы загрузки таблиц:');
    FOR rec IN (
        SELECT table_name, load_status, records_processed, 
               TO_CHAR(last_load_date, 'DD.MM.YYYY HH24:MI') as last_load
        FROM idl.load_tracking
        ORDER BY table_name
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  - ' || rec.table_name || ': ' || rec.load_status || 
                           ' (записей: ' || rec.records_processed || 
                           ', время: ' || rec.last_load || ')');
    END LOOP;
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || 'ОШИБКА при выполнении загрузки IDL: ' || SQLERRM);
        
        -- Выводим статусы при ошибке
        DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Статусы загрузки таблиц при ошибке:');
        FOR rec IN (
            SELECT table_name, load_status, error_message
            FROM idl.load_tracking
            WHERE load_status != 'COMPLETED'
            ORDER BY table_name
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  - ' || rec.table_name || ': ' || rec.load_status);
            IF rec.error_message IS NOT NULL THEN
                DBMS_OUTPUT.PUT_LINE('    Ошибка: ' || SUBSTR(rec.error_message, 1, 100));
            END IF;
        END LOOP;
        
        RAISE;
END run_complete_idl_load;
/

-- 5. Исправленная процедура для инкрементальной загрузки IDL
CREATE OR REPLACE PROCEDURE flow.run_incremental_idl_load
IS
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_last_load_date DATE;
BEGIN
    v_start_time := SYSTIMESTAMP;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Запуск инкрементальной загрузки IDL слоя');
    DBMS_OUTPUT.PUT_LINE('Время начала: ' || TO_CHAR(v_start_time, 'DD.MM.YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Определяем дату последней загрузки для транзакций
    BEGIN
        SELECT last_load_date 
        INTO v_last_load_date
        FROM idl.load_tracking 
        WHERE table_name = 'BRIDGE_ACCOUNT_TRANSACTION'
          AND load_status = 'COMPLETED'
          AND ROWNUM = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_last_load_date := DATE '1900-01-01';
    END;
    
    DBMS_OUTPUT.PUT_LINE('Последняя успешная загрузка: ' || 
                         TO_CHAR(v_last_load_date, 'DD.MM.YYYY HH24:MI:SS'));
    
    -- Шаг 1: Инкрементальная загрузка бриджа счет-транзакция
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '1. Загрузка бриджа счет-транзакция...');
    flow.load_bridge_account_transaction;
    
    -- Шаг 2: Полная загрузка бриджа иерархии (если изменились счета)
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '2. Загрузка бриджа иерархии счетов...');
    flow.load_bridge_account_hierarchy;
    
    -- Шаг 3: Пересчет агрегированных данных
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '3. Загрузка бриджа агрегированных данных...');
    flow.load_bridge_account_aggregation;
    
    v_end_time := SYSTIMESTAMP;
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '========================================');
    DBMS_OUTPUT.PUT_LINE('Инкрементальная загрузка IDL завершена');
    DBMS_OUTPUT.PUT_LINE('Время окончания: ' || TO_CHAR(v_end_time, 'DD.MM.YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Общее время выполнения: ' || 
                         EXTRACT(MINUTE FROM (v_end_time - v_start_time)) || ' мин. ' ||
                         EXTRACT(SECOND FROM (v_end_time - v_start_time)) || ' сек.');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || 'ОШИБКА при выполнении инкрементальной загрузки: ' || SQLERRM);
        RAISE;
END run_incremental_idl_load;
/