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


create or replace PROCEDURE      flow.load_account_from_stg_to_rdv
IS
    -- Переменные для логирования и обработки ошибок
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_hub_inserted      NUMBER := 0;
    v_sat_updated       NUMBER := 0;
    v_sat_inserted      NUMBER := 0;
    v_error_message     VARCHAR2(4000);
    
    -- Курсор для получения данных из STG
    CURSOR c_account_data IS
        SELECT 
            st.account_id,
            LOWER(STANDARD_HASH(st.account_id, 'MD5')) as hsh,
            st.account_name,
            st.start_balance,
            st.hash_diff,
            st.record_source
        FROM stg.stg_account st;

BEGIN
    -- Логирование начала загрузки
    DBMS_OUTPUT.PUT_LINE('Начало загрузки счетов из STG в RDV: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));

    -- Блок обработки данных с обработкой исключений
    BEGIN
        FOR rec IN c_account_data LOOP
            BEGIN
                -- Шаг 1: Вычисляем хэш-ключ для хаба
                DECLARE
                    v_account_hash_key VARCHAR2(64);
                BEGIN
                    v_account_hash_key := rec.hsh;
                    
                    -- Шаг 2: Загрузка в хаб H_ACCOUNT (если записи нет)
                    INSERT INTO rdv.h_account (
                        account_hash_key,
                        account_id,
                        load_date,
                        record_source
                    )
                    VALUES (
                        v_account_hash_key,
                        rec.account_id,
                        v_load_timestamp,
                        rec.record_source
                    );
                    
                    v_hub_inserted := v_hub_inserted + 1;
                    
                EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN
                        -- Запись уже существует в хабе - это нормально
                        NULL;
                    WHEN OTHERS THEN
                        RAISE;
                END;
                
                -- Шаг 3: Загрузка в спутник S_ACCOUNT по SCD Type 2
                DECLARE
                    v_account_hash_key VARCHAR2(64);
                    v_current_hash_diff VARCHAR2(64);
                    v_current_load_date DATE;
                BEGIN
                    -- Получаем хэш-ключ
                    v_account_hash_key := rec.hsh;
                    
                    -- Проверяем текущую актуальную версию в спутнике
                    BEGIN
                        SELECT hash_diff, load_date
                        INTO v_current_hash_diff, v_current_load_date
                        FROM rdv.s_account
                        WHERE account_hash_key = v_account_hash_key
                          AND load_end_date IS NULL;  -- Актуальная запись
                          
                        -- Если хэш изменился, нужно создать новую версию
                        IF v_current_hash_diff != rec.hash_diff THEN
                            -- Закрываем текущую актуальную версию
                            UPDATE rdv.s_account 
                            SET load_end_date = v_load_timestamp
                            WHERE account_hash_key = v_account_hash_key
                              AND load_end_date IS NULL;
                              
                            v_sat_updated := v_sat_updated + 1;
                            
                            -- Создаем новую версию
                            INSERT INTO rdv.s_account (
                                account_hash_key,
                                load_date,
                                hash_diff,
                                load_end_date,
                                account_name,
                                start_balance,
                                record_source
                            )
                            VALUES (
                                v_account_hash_key,
                                v_load_timestamp,
                                rec.hash_diff,
                                NULL,  -- Новая актуальная версия
                                rec.account_name,
                                rec.start_balance,
                                rec.record_source
                            );
                            
                            v_sat_inserted := v_sat_inserted + 1;
                            v_records_processed := v_records_processed + 1;
                        END IF;
                        
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            -- Актуальной записи нет - вставляем первую версию
                            INSERT INTO rdv.s_account (
                                account_hash_key,
                                load_date,
                                hash_diff,
                                load_end_date,
                                account_name,
                                start_balance,
                                record_source
                            )
                            VALUES (
                                v_account_hash_key,
                                v_load_timestamp,
                                rec.hash_diff,
                                NULL,  -- Первая актуальная версия
                                rec.account_name,
                                rec.start_balance,
                                rec.record_source
                            );
                            
                            v_sat_inserted := v_sat_inserted + 1;
                            v_records_processed := v_records_processed + 1;
                        WHEN TOO_MANY_ROWS THEN
                            -- Несколько актуальных записей - ошибка в данных
                            v_error_message := 'Найдено несколько актуальных версий для account_id=' || rec.account_id;
                            DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                            -- Записываем в лог ошибок
                            INSERT INTO flow.load_error_log (
                                procedure_name,
                                error_message, 
                                account_id,
                                record_data
                            ) VALUES (
                                'LOAD_ACCOUNT_FROM_STG_TO_RDV',
                                v_error_message,
                                rec.account_id,
                                'account_name=' || rec.account_name || ', start_balance=' || rec.start_balance
                            );
                    END;
                    
                END; -- Конец внутреннего блока DECLARE
                
                -- Логирование прогресса
               -- IF MOD(v_records_processed, 1000) = 0 THEN
               --     DBMS_OUTPUT.PUT_LINE('Обработано записей: ' || v_records_processed);
               -- END IF;
                
            EXCEPTION
                WHEN OTHERS THEN
                    -- Логируем ошибку и продолжаем обработку
                    v_error_message := 'Ошибка при обработке account_id=' || rec.account_id || ': ' || SQLERRM;
                    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                    
                    -- Записываем в лог ошибок
                    INSERT INTO flow.load_error_log (
                        procedure_name,
                        error_message,
                        account_id, 
                        record_data
                    ) VALUES (
                        'LOAD_ACCOUNT_FROM_STG_TO_RDV',
                        v_error_message,
                        rec.account_id,
                        'account_name=' || rec.account_name || ', start_balance=' || rec.start_balance
                    );
                    
                    CONTINUE; -- Продолжаем обработку следующих записей
            END;
        END LOOP;
        
        COMMIT; -- Фиксируем все изменения
        
        -- Логирование успешного завершения
        DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
        DBMS_OUTPUT.PUT_LINE('Статистика:');
        DBMS_OUTPUT.PUT_LINE('  - Вставлено в хаб: ' || v_hub_inserted);
        DBMS_OUTPUT.PUT_LINE('  - Обновлено в спутнике: ' || v_sat_updated);
        DBMS_OUTPUT.PUT_LINE('  - Вставлено в спутнике: ' || v_sat_inserted);
        DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK; -- Откатываем изменения при ошибке
            v_error_message := 'Критическая ошибка при загрузке: ' || SQLERRM;
            DBMS_OUTPUT.PUT_LINE('Критическая ошибка: ' || v_error_message);
            RAISE; -- Пробрасываем исключение дальше
    END;
  
END load_account_from_stg_to_rdv;
/

CREATE OR REPLACE PROCEDURE flow.load_account_hierarchy
IS
    v_load_timestamp DATE := SYSDATE;
    v_records_processed NUMBER := 0;
BEGIN
    -- Вставляем связи "родитель-потомок" на основе структуры account_id
    FOR rec IN (
        select SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)) parent_account_id, account_id child_account_id,
              LOWER(STANDARD_HASH(SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)), 'MD5')) as parent_hash_key,
              LOWER(STANDARD_HASH(account_id, 'MD5')) as child_hash_key
        from stg.stg_account
        where SUBSTR(account_id, 1, INSTR(account_id,'.', -1, 2)) is not null
    ) LOOP
        BEGIN
            INSERT INTO rdv.l_account_account (
                account_parent_child_hash_key,
                parent_account_hash_key,
                child_account_hash_key,
                load_date,
                record_source
            )
            VALUES (
                LOWER(STANDARD_HASH(rec.parent_hash_key || '|' || rec.child_hash_key, 'MD5')),
                rec.parent_hash_key,
                rec.child_hash_key,
                v_load_timestamp,
                'SRC.ACCOUNT'
            );
            
            v_records_processed := v_records_processed + 1;
            
        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN
                NULL; -- Связь уже существует
        END;
    END LOOP;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Загружено иерархических связей: ' || v_records_processed);
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Ошибка при загрузке иерархии: ' || SQLERRM);
        RAISE;
END load_account_hierarchy;
/

CREATE OR REPLACE PROCEDURE flow.load_transaction_from_stg_to_rdv
IS
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_hub_inserted      NUMBER := 0;
    v_error_message     VARCHAR2(4000);
    
    CURSOR c_transaction_data IS
        SELECT 
            st.transaction_id,
            LOWER(STANDARD_HASH(st.transaction_id, 'MD5')) as transaction_hash_key,
            st.record_source
        FROM stg.stg_transaction st
        WHERE st.load_date >= TRUNC(SYSDATE-5) -- Загрузка за последние 30 дней
           OR NOT EXISTS (
                SELECT 1 FROM rdv.h_transaction ht 
                WHERE ht.transaction_id = st.transaction_id
           );

BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки транзакций из STG в RDV: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));

    BEGIN
        FOR rec IN c_transaction_data LOOP
            BEGIN
                -- Вставка в хаб H_TRANSACTION
                INSERT INTO rdv.h_transaction (
                    transaction_hash_key,
                    transaction_id,
                    load_date,
                    record_source
                )
                VALUES (
                    rec.transaction_hash_key,
                    rec.transaction_id,
                    v_load_timestamp,
                    rec.record_source
                );
                
                v_hub_inserted := v_hub_inserted + 1;
                v_records_processed := v_records_processed + 1;
                
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    -- Запись уже существует - это нормально
                    NULL;
                WHEN OTHERS THEN
                    v_error_message := 'Ошибка при обработке transaction_id=' || rec.transaction_id || ': ' || SQLERRM;
                    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                    
                    INSERT INTO flow.load_error_log (
                        procedure_name,
                        error_message,
                        transaction_id,
                        record_data
                    ) VALUES (
                        'LOAD_TRANSACTION_FROM_STG_TO_RDV',
                        v_error_message,
                        rec.transaction_id,
                        'transaction_id=' || rec.transaction_id
                    );
            END;
            
            -- Логирование прогресса
--            IF MOD(v_records_processed, 1000) = 0 THEN
--                DBMS_OUTPUT.PUT_LINE('Обработано записей: ' || v_records_processed);
--            END IF;
        END LOOP;
        
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
        DBMS_OUTPUT.PUT_LINE('Статистика:');
        DBMS_OUTPUT.PUT_LINE('  - Вставлено в хаб транзакций: ' || v_hub_inserted);
        DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            v_error_message := 'Критическая ошибка при загрузке транзакций: ' || SQLERRM;
            DBMS_OUTPUT.PUT_LINE('Критическая ошибка: ' || v_error_message);
            RAISE;
    END;
    
END load_transaction_from_stg_to_rdv;
/

CREATE OR REPLACE PROCEDURE flow.load_account_transaction_link
IS
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_link_inserted     NUMBER := 0;
    v_error_message     VARCHAR2(4000);
    
    CURSOR c_transaction_data IS
        SELECT 
            st.transaction_id,
            st.debit_account_id,
            st.credit_account_id,
            LOWER(STANDARD_HASH(st.transaction_id, 'MD5')) as transaction_hash_key,
            LOWER(STANDARD_HASH(st.debit_account_id, 'MD5')) as debit_account_hash_key,
            LOWER(STANDARD_HASH(st.credit_account_id, 'MD5')) as credit_account_hash_key,
            LOWER(STANDARD_HASH(st.debit_account_id || '|' || st.credit_account_id || '|' || st.transaction_id, 'MD5')) as account_txn_hash_key,
            st.record_source
        FROM stg.stg_transaction st
        WHERE EXISTS (
            SELECT 1 FROM rdv.h_account ha WHERE ha.account_id = st.debit_account_id
        )
        AND EXISTS (
            SELECT 1 FROM rdv.h_account ha WHERE ha.account_id = st.credit_account_id
        );

BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки связей счет-транзакция: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));

    BEGIN
        FOR rec IN c_transaction_data LOOP
            BEGIN
                -- Вставка в связь L_ACCOUNT_TRANSACTION
                INSERT INTO rdv.l_account_transaction (
                    account_txn_hash_key,
                    debit_account_hash_key,
                    credit_account_hash_key,
                    transaction_hash_key,
                    load_date,
                    record_source
                )
                VALUES (
                    rec.account_txn_hash_key,
                    rec.debit_account_hash_key,
                    rec.credit_account_hash_key,
                    rec.transaction_hash_key,
                    v_load_timestamp,
                    rec.record_source
                );
                
                v_link_inserted := v_link_inserted + 1;
                v_records_processed := v_records_processed + 1;
                
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    -- Связь уже существует - это нормально
                    NULL;
                WHEN OTHERS THEN
                    v_error_message := 'Ошибка при обработке transaction_id=' || rec.transaction_id 
                                     || ', debit=' || rec.debit_account_id 
                                     || ', credit=' || rec.credit_account_id 
                                     || ', trans_hash=' || rec.transaction_hash_key 
                                     || ': ' || SQLERRM;
                    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                    
                    INSERT INTO flow.load_error_log (
                        procedure_name,
                        error_message,
                        transaction_id,
                        record_data
                    ) VALUES (
                        'LOAD_ACCOUNT_TRANSACTION_LINK',
                        v_error_message,
                        rec.transaction_id,
                        'debit_account_id=' || rec.debit_account_id 
                        || ', credit_account_id=' || rec.credit_account_id
                        || ', transaction_id=' || rec.transaction_id
                    );
            END;
            
            -- Логирование прогресса
            --IF MOD(v_records_processed, 1000) = 0 THEN
            --   DBMS_OUTPUT.PUT_LINE('Обработано записей: ' || v_records_processed);
            --END IF;
        END LOOP;
        
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
        DBMS_OUTPUT.PUT_LINE('Статистика:');
        DBMS_OUTPUT.PUT_LINE('  - Вставлено связей: ' || v_link_inserted);
        DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
        
        -- Логирование транзакций, которые не были загружены из-за отсутствия счетов
        DECLARE
            v_missing_records NUMBER;
        BEGIN
            SELECT COUNT(*)
            INTO v_missing_records
            FROM stg.stg_transaction st
            WHERE NOT EXISTS (
                SELECT 1 FROM rdv.h_account ha WHERE ha.account_id = st.debit_account_id
            )
            OR NOT EXISTS (
                SELECT 1 FROM rdv.h_account ha WHERE ha.account_id = st.credit_account_id
            );
            
            IF v_missing_records > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Внимание: ' || v_missing_records || ' транзакций не загружены из-за отсутствующих счетов.');
                
                -- Логируем эти записи
                INSERT INTO flow.load_error_log (
                    procedure_name,
                    error_message,
                    transaction_id,
                    record_data
                )
                SELECT 
                    'LOAD_ACCOUNT_TRANSACTION_LINK',
                    'Отсутствуют связанные счета',
                    st.transaction_id,
                    'debit_account_id=' || st.debit_account_id 
                    || ', credit_account_id=' || st.credit_account_id
                    || ', transaction_id=' || st.transaction_id
                FROM stg.stg_transaction st
                WHERE NOT EXISTS (
                    SELECT 1 FROM rdv.h_account ha WHERE ha.account_id = st.debit_account_id
                )
                OR NOT EXISTS (
                    SELECT 1 FROM rdv.h_account ha WHERE ha.account_id = st.credit_account_id
                );
                
                COMMIT;
            END IF;
        END;
        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            v_error_message := 'Критическая ошибка при загрузке связей: ' || SQLERRM;
            DBMS_OUTPUT.PUT_LINE('Критическая ошибка: ' || v_error_message);
            RAISE;
    END;
    
END load_account_transaction_link;
/

CREATE OR REPLACE PROCEDURE flow.load_account_transaction_satellite
IS
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_sat_updated       NUMBER := 0;
    v_sat_inserted      NUMBER := 0;
    v_error_message     VARCHAR2(4000);
    
    CURSOR c_transaction_data IS
        SELECT 
            st.transaction_id,
            st.transaction_date,
            st.amount,
            st.hash_diff,
            st.record_source,
            lat.account_txn_hash_key
        FROM stg.stg_transaction st
        JOIN rdv.l_account_transaction lat ON 
            lat.debit_account_hash_key = LOWER(STANDARD_HASH(st.debit_account_id, 'MD5'))
            AND lat.credit_account_hash_key = LOWER(STANDARD_HASH(st.credit_account_id, 'MD5'))
            AND lat.transaction_hash_key = LOWER(STANDARD_HASH(st.transaction_id, 'MD5'));

BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки сателлита связей счет-транзакция: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));

    BEGIN
        FOR rec IN c_transaction_data LOOP
            BEGIN
                -- Проверяем текущую актуальную версию в спутнике
                DECLARE
                    v_current_hash_diff VARCHAR2(64);
                    v_current_load_date DATE;
                BEGIN
                    -- Пытаемся найти актуальную запись
                    BEGIN
                        SELECT hash_diff, load_date
                        INTO v_current_hash_diff, v_current_load_date
                        FROM rdv.s_account_transaction
                        WHERE account_txn_hash_key = rec.account_txn_hash_key
                          AND load_end_date IS NULL;  -- Актуальная запись
                          
                        -- Если хэш изменился, нужно создать новую версию
                        IF v_current_hash_diff != rec.hash_diff THEN
                            -- Закрываем текущую актуальную версию
                            UPDATE rdv.s_account_transaction 
                            SET load_end_date = v_load_timestamp
                            WHERE account_txn_hash_key = rec.account_txn_hash_key
                              AND load_end_date IS NULL;
                              
                            v_sat_updated := v_sat_updated + 1;
                            
                            -- Создаем новую версию
                            INSERT INTO rdv.s_account_transaction (
                                account_txn_hash_key,
                                load_date,
                                hash_diff,
                                load_end_date,
                                transaction_date,
                                amount,
                                record_source
                            )
                            VALUES (
                                rec.account_txn_hash_key,
                                v_load_timestamp,
                                rec.hash_diff,
                                NULL,  -- Новая актуальная версия
                                rec.transaction_date,
                                rec.amount,
                                rec.record_source
                            );
                            
                            v_sat_inserted := v_sat_inserted + 1;
                            v_records_processed := v_records_processed + 1;
                        END IF;
                        
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            -- Актуальной записи нет - вставляем первую версию
                            INSERT INTO rdv.s_account_transaction (
                                account_txn_hash_key,
                                load_date,
                                hash_diff,
                                load_end_date,
                                transaction_date,
                                amount,
                                record_source
                            )
                            VALUES (
                                rec.account_txn_hash_key,
                                v_load_timestamp,
                                rec.hash_diff,
                                NULL,  -- Первая актуальная версия
                                rec.transaction_date,
                                rec.amount,
                                rec.record_source
                            );
                            
                            v_sat_inserted := v_sat_inserted + 1;
                            v_records_processed := v_records_processed + 1;
                        WHEN TOO_MANY_ROWS THEN
                            -- Несколько актуальных записей - ошибка в данных
                            v_error_message := 'Найдено несколько актуальных версий для account_txn_hash_key=' || rec.account_txn_hash_key;
                            DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                            
                            INSERT INTO flow.load_error_log (
                                procedure_name,
                                error_message,
                                transaction_id,
                                record_data
                            ) VALUES (
                                'LOAD_ACCOUNT_TRANSACTION_SATELLITE',
                                v_error_message,
                                rec.transaction_id,
                                'transaction_date=' || TO_CHAR(rec.transaction_date, 'DD.MM.YYYY')
                                || ', amount=' || rec.amount
                            );
                    END;
                END;
                
                -- Логирование прогресса
                --IF MOD(v_records_processed, 1000) = 0 THEN
                --    DBMS_OUTPUT.PUT_LINE('Обработано записей: ' || v_records_processed);
                --END IF;
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_message := 'Ошибка при обработке transaction_id=' || rec.transaction_id || ': ' || SQLERRM;
                    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                    
                    INSERT INTO flow.load_error_log (
                        procedure_name,
                        error_message,
                        transaction_id,
                        record_data
                    ) VALUES (
                        'LOAD_ACCOUNT_TRANSACTION_SATELLITE',
                        v_error_message,
                        rec.transaction_id,
                        'transaction_date=' || TO_CHAR(rec.transaction_date, 'DD.MM.YYYY')
                        || ', amount=' || rec.amount
                    );
            END;
        END LOOP;
        
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Загрузка завершена успешно.');
        DBMS_OUTPUT.PUT_LINE('Статистика:');
        DBMS_OUTPUT.PUT_LINE('  - Обновлено в спутнике: ' || v_sat_updated);
        DBMS_OUTPUT.PUT_LINE('  - Вставлено в спутнике: ' || v_sat_inserted);
        DBMS_OUTPUT.PUT_LINE('  - Всего обработано записей: ' || v_records_processed);
        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            v_error_message := 'Критическая ошибка при загрузке сателлита: ' || SQLERRM;
            DBMS_OUTPUT.PUT_LINE('Критическая ошибка: ' || v_error_message);
            RAISE;
    END;
    
END load_account_transaction_satellite;
/

CREATE OR REPLACE PROCEDURE flow.load_pit_account (
    p_snapshot_date IN DATE DEFAULT NULL
)
IS
    v_snapshot_date     DATE := NVL(p_snapshot_date, TRUNC(SYSDATE));
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_error_message     VARCHAR2(4000);
    
    -- Курсор для получения актуальных версий счетов на snapshot_date
    CURSOR c_account_snapshots IS
        WITH current_sat_versions AS (
            SELECT 
                ha.account_hash_key,
                ha.account_id,
                sa.load_date as sat_load_date,
                sa.hash_diff as sat_hash_key,
                ROW_NUMBER() OVER (
                    PARTITION BY ha.account_hash_key 
                    ORDER BY sa.load_date DESC
                ) as rn
            FROM rdv.h_account ha
            JOIN rdv.s_account sa ON ha.account_hash_key = sa.account_hash_key
            WHERE sa.load_date <= v_snapshot_date
              AND (sa.load_end_date IS NULL OR sa.load_end_date > v_snapshot_date)
        )
        SELECT 
            account_hash_key,
            sat_load_date,
            sat_hash_key
        FROM current_sat_versions
        WHERE rn = 1;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки PIT-таблицы для счетов на дату: ' || TO_CHAR(v_snapshot_date, 'DD.MM.YYYY'));
    DBMS_OUTPUT.PUT_LINE('Время загрузки: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));

    -- Проверяем, есть ли уже запись для этой даты
    DECLARE
        v_existing_records NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_existing_records
        FROM rdv.pit_account
        WHERE snapshot_date = v_snapshot_date;
        
        IF v_existing_records > 0 THEN
            DBMS_OUTPUT.PUT_LINE('Внимание: Для даты ' || TO_CHAR(v_snapshot_date, 'DD.MM.YYYY') 
                               || ' уже существуют записи (' || v_existing_records || ' шт.).');
            DBMS_OUTPUT.PUT_LINE('Удаление существующих записей...');
            
            DELETE FROM rdv.pit_account
            WHERE snapshot_date = v_snapshot_date;
            
            DBMS_OUTPUT.PUT_LINE('Удалено записей: ' || SQL%ROWCOUNT);
        END IF;
    END;

    BEGIN
        FOR rec IN c_account_snapshots LOOP
            BEGIN
                -- Вставка в PIT-таблицу
                INSERT INTO rdv.pit_account (
                    account_hash_key,
                    snapshot_date,
                    s_account_hash_key,
                    load_date
                )
                VALUES (
                    rec.account_hash_key,
                    v_snapshot_date,
                    rec.sat_hash_key,
                    v_load_timestamp
                );
                
                v_records_processed := v_records_processed + 1;
                
                -- Логирование прогресса
                IF MOD(v_records_processed, 1000) = 0 THEN
                    DBMS_OUTPUT.PUT_LINE('Обработано записей: ' || v_records_processed);
                END IF;
                
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    -- Дублирующая запись (маловероятно после удаления)
                    v_error_message := 'Дублирующая запись для account_hash_key=' || rec.account_hash_key;
                    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                WHEN OTHERS THEN
                    v_error_message := 'Ошибка при обработке account_hash_key=' || rec.account_hash_key || ': ' || SQLERRM;
                    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                    
                    INSERT INTO flow.load_error_log (
                        procedure_name,
                        error_message,
                        record_data
                    ) VALUES (
                        'LOAD_PIT_ACCOUNT',
                        v_error_message,
                        'account_hash_key=' || rec.account_hash_key
                        || ', snapshot_date=' || TO_CHAR(v_snapshot_date, 'DD.MM.YYYY')
                    );
            END;
        END LOOP;
        
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Загрузка PIT-таблицы завершена успешно.');
        DBMS_OUTPUT.PUT_LINE('Всего загружено записей: ' || v_records_processed);
        
        -- Создаем индекс для улучшения производительности запросов
        DBMS_OUTPUT.PUT_LINE('Оптимизация индексов...');
        EXECUTE IMMEDIATE 'ALTER INDEX RDV.PIT_ACCOUNT_PK REBUILD';
        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            v_error_message := 'Критическая ошибка при загрузке PIT-таблицы: ' || SQLERRM;
            DBMS_OUTPUT.PUT_LINE('Критическая ошибка: ' || v_error_message);
            RAISE;
    END;
    
END load_pit_account;
/

CREATE OR REPLACE PROCEDURE flow.load_pit_account (
    p_snapshot_date IN DATE DEFAULT NULL
)
IS
    v_snapshot_date     DATE := NVL(p_snapshot_date, TRUNC(SYSDATE));
    v_load_timestamp    DATE := SYSDATE;
    v_records_processed NUMBER := 0;
    v_error_message     VARCHAR2(4000);
    
    -- Курсор для получения актуальных версий счетов на snapshot_date
    CURSOR c_account_snapshots IS
        WITH current_sat_versions AS (
            SELECT 
                ha.account_hash_key,
                ha.account_id,
                sa.load_date as sat_load_date,
                sa.hash_diff as sat_hash_key,
                ROW_NUMBER() OVER (
                    PARTITION BY ha.account_hash_key 
                    ORDER BY sa.load_date DESC
                ) as rn
            FROM rdv.h_account ha
            JOIN rdv.s_account sa ON ha.account_hash_key = sa.account_hash_key
            WHERE sa.load_date <= v_snapshot_date
              AND (sa.load_end_date IS NULL OR sa.load_end_date > v_snapshot_date)
        )
        SELECT 
            account_hash_key,
            sat_load_date,
            sat_hash_key
        FROM current_sat_versions
        WHERE rn = 1;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Начало загрузки PIT-таблицы для счетов на дату: ' || TO_CHAR(v_snapshot_date, 'DD.MM.YYYY'));
    DBMS_OUTPUT.PUT_LINE('Время загрузки: ' || TO_CHAR(v_load_timestamp, 'DD.MM.YYYY HH24:MI:SS'));

    -- Удаляем существующие записи для этой даты
    DELETE FROM rdv.pit_account
    WHERE snapshot_date = v_snapshot_date;
    
    v_records_processed := SQL%ROWCOUNT;
    
    IF v_records_processed > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Удалено существующих записей: ' || v_records_processed);
    END IF;

    BEGIN
        FOR rec IN c_account_snapshots LOOP
            BEGIN
                -- Вставка в PIT-таблицу
                INSERT INTO rdv.pit_account (
                    account_hash_key,
                    snapshot_date,
                    s_account_hash_key,
                    load_date
                )
                VALUES (
                    rec.account_hash_key,
                    v_snapshot_date,
                    rec.sat_hash_key,
                    v_load_timestamp
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_message := 'Ошибка при обработке account_hash_key=' || rec.account_hash_key || ': ' || SQLERRM;
                    DBMS_OUTPUT.PUT_LINE('Ошибка: ' || v_error_message);
                    
                    INSERT INTO flow.load_error_log (
                        procedure_name,
                        error_message,
                        record_data
                    ) VALUES (
                        'LOAD_PIT_ACCOUNT',
                        v_error_message,
                        'account_hash_key=' || rec.account_hash_key
                        || ', snapshot_date=' || TO_CHAR(v_snapshot_date, 'DD.MM.YYYY')
                    );
            END;
        END LOOP;
        
        v_records_processed := SQL%ROWCOUNT;
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Загрузка PIT-таблицы завершена успешно.');
        DBMS_OUTPUT.PUT_LINE('Всего загружено записей: ' || v_records_processed);
        
        -- Анализируем таблицу для обновления статистики
/*        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => 'RDV',
            tabname => 'PIT_ACCOUNT',
            estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
            method_opt => 'FOR ALL COLUMNS SIZE AUTO',
            cascade => TRUE
        );
        
        DBMS_OUTPUT.PUT_LINE('Статистика таблицы обновлена.');
*/        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            v_error_message := 'Критическая ошибка при загрузке PIT-таблицы: ' || SQLERRM;
            DBMS_OUTPUT.PUT_LINE('Критическая ошибка: ' || v_error_message);
            RAISE;
    END;
    
END load_pit_account;
/

CREATE OR REPLACE PROCEDURE flow.run_complete_dv_load
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
    flow.load_account_from_stg_to_rdv;
    
    -- Шаг 2: Загрузка иерархии счетов
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '2. Загрузка иерархии счетов...');
    flow.load_account_hierarchy;
    
    -- Шаг 3: Загрузка транзакций
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '3. Загрузка транзакций...');
    flow.load_transaction_from_stg_to_rdv;
    
    -- Шаг 4: Загрузка связей счет-транзакция
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '4. Загрузка связей счет-транзакция...');
    flow.load_account_transaction_link;
    
    -- Шаг 5: Загрузка сателлита связей
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '5. Загрузка сателлита связей счет-транзакция...');
    flow.load_account_transaction_satellite;
    
    -- Шаг 6: Загрузка PIT-таблицы
--    DBMS_OUTPUT.PUT_LINE(CHR(10) || '6. Загрузка PIT-таблицы для счетов...');
--    flow.load_pit_account(TRUNC(SYSDATE));
    
    v_end_time := SYSTIMESTAMP;
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '========================================');
    DBMS_OUTPUT.PUT_LINE('Загрузка Data Vault завершена');
    DBMS_OUTPUT.PUT_LINE('Время окончания: ' || TO_CHAR(v_end_time, 'DD.MM.YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Общее время выполнения: ' || 
                         EXTRACT(MINUTE FROM (v_end_time - v_start_time)) || ' мин. ' ||
                         EXTRACT(SECOND FROM (v_end_time - v_start_time)) || ' сек.');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || 'ОШИБКА при выполнении загрузки: ' || SQLERRM);
        RAISE;
END run_complete_dv_load;
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