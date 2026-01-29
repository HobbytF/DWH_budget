-- 1. Бридж для связи счетов с транзакциями
CREATE TABLE IDL.bridge_account_transaction (
    transaction_id          NUMBER(10) NOT NULL,
    transaction_date        DATE NOT NULL,
    amount                  NUMBER(10,2) NOT NULL,
    debit_account_id        VARCHAR2(16) NOT NULL,
    debit_account_name      VARCHAR2(50),
    debit_start_balance     NUMBER(10,2),
    debit_parent_account_id VARCHAR2(16),
    credit_account_id       VARCHAR2(16) NOT NULL,
    credit_account_name     VARCHAR2(50),
    credit_start_balance    NUMBER(10,2),
    credit_parent_account_id VARCHAR2(16),
    load_date               DATE NOT NULL,
    hash_key                VARCHAR2(64),
    
    CONSTRAINT pk_bridge_acc_txn PRIMARY KEY (hash_key)
);

-- Индексы для ускорения запросов
CREATE INDEX idx_bridge_acc_txn_date ON IDL.bridge_account_transaction (transaction_date);
CREATE INDEX idx_bridge_acc_txn_debit ON IDL.bridge_account_transaction (debit_account_id);
CREATE INDEX idx_bridge_acc_txn_credit ON IDL.bridge_account_transaction (credit_account_id);

-- 1.1. Бридж для связи счетов с транзакциями
CREATE TABLE IDL.bridge_transaction_account (
    transaction_hash_key    VARCHAR2(64),
    transaction_id          NUMBER(10) NOT NULL,
    transaction_date        DATE NOT NULL,
    amount                  NUMBER(10,2) NOT NULL,
    debit_account_id        VARCHAR2(16) NOT NULL,
    debit_account_hash_key  VARCHAR2(64),
    credit_account_id       VARCHAR2(16) NOT NULL,
    credit_account_hash_key VARCHAR2(64),
    effective_date_from     DATE NOT NULL,
    effective_date_to       DATE NOT NULL,
    hash_key                VARCHAR2(64),
    
    CONSTRAINT pk_bridge_acc_txn PRIMARY KEY (hash_key)
);

-- Индексы для ускорения запросов
CREATE INDEX idx_bridge_acc_txa_date ON IDL.bridge_transaction_account (transaction_date);
CREATE INDEX idx_bridge_acc_txa_debit ON IDL.bridge_transaction_account (debit_account_id);
CREATE INDEX idx_bridge_acc_txa_credit ON IDL.bridge_transaction_account (credit_account_id);


-- 2. Бридж для иерархии счетов
CREATE TABLE IDL.bridge_account_hierarchy (
    account_id              VARCHAR2(16) NOT NULL,
    account_name            VARCHAR2(50),
    start_balance           NUMBER(10,2),
    parent_account_id       VARCHAR2(16),
    parent_account_name     VARCHAR2(50),
    level_number            NUMBER(2) NOT NULL,
    path_string             VARCHAR2(1000) NOT NULL,
    load_date               DATE NOT NULL,
    hash_key                VARCHAR2(64),
    
    CONSTRAINT pk_bridge_acc_hier PRIMARY KEY (hash_key)
);

CREATE INDEX idx_bridge_acc_hier_id ON IDL.bridge_account_hierarchy (account_id);
CREATE INDEX idx_bridge_acc_hier_parent ON IDL.bridge_account_hierarchy (parent_account_id);
CREATE INDEX idx_bridge_acc_hier_level ON IDL.bridge_account_hierarchy (level_number);

-- 3. Бридж для агрегированных данных по счетам
CREATE TABLE IDL.bridge_account_aggregation (
    account_id              VARCHAR2(16) NOT NULL,
    account_name            VARCHAR2(50),
    start_balance           NUMBER(10,2),
    total_debit_amount      NUMBER(15,2) DEFAULT 0,
    total_credit_amount     NUMBER(15,2) DEFAULT 0,
    transaction_count       NUMBER(10) DEFAULT 0,
    last_transaction_date   DATE,
    load_date               DATE NOT NULL,
    hash_key                VARCHAR2(64),
    
    CONSTRAINT pk_bridge_acc_agg PRIMARY KEY (hash_key)
);

CREATE INDEX idx_bridge_acc_agg_id ON IDL.bridge_account_aggregation (account_id);

-- 4. Таблица для отслеживания состояния загрузки
CREATE TABLE IDL.load_tracking (
    table_name              VARCHAR2(50) PRIMARY KEY,
    last_load_date          DATE NOT NULL,
    last_processed_hash     VARCHAR2(64),
    records_processed       NUMBER(10) DEFAULT 0,
    load_status             VARCHAR2(20) DEFAULT 'COMPLETED',
    error_message           VARCHAR2(4000)
);

-- Инициализация таблицы отслеживания (используем MERGE для безопасной вставки)
MERGE INTO IDL.load_tracking tgt
USING (
    SELECT 'BRIDGE_ACCOUNT_TRANSACTION' as table_name, DATE '1900-01-01' as last_load_date FROM dual UNION ALL
    SELECT 'BRIDGE_ACCOUNT_HIERARCHY', DATE '1900-01-01' FROM dual UNION ALL
    SELECT 'BRIDGE_ACCOUNT_AGGREGATION', DATE '1900-01-01' FROM dual
) src
ON (tgt.table_name = src.table_name)
WHEN NOT MATCHED THEN
    INSERT (table_name, last_load_date)
    VALUES (src.table_name, src.last_load_date);

COMMIT;