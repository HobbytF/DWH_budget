-- СОЗДАНИЕ ХАБА СЧЕТОВ
CREATE TABLE RDV.h_account (
    account_hash_key    VARCHAR2(64) PRIMARY KEY, -- Хэш от бизнес-ключа (account_id)
    account_id          VARCHAR2(16) NOT NULL,    -- Исходный бизнес-ключ
    load_date           DATE NOT NULL,
    record_source       VARCHAR2(50) NOT NULL
);
-- Уникальный индекс на бизнес-ключ
CREATE UNIQUE INDEX RDV.bk_h_account ON RDV.h_account (account_id);

-- СОЗДАНИЕ ХАБА ТРАНЗАКЦИЙ
CREATE TABLE RDV.h_transaction (
    transaction_hash_key    VARCHAR2(64) PRIMARY KEY, -- Хэш от transaction_id
    transaction_id          NUMBER(10) NOT NULL,
    load_date               DATE NOT NULL,
    record_source           VARCHAR2(50) NOT NULL
);
CREATE UNIQUE INDEX RDV.bk_h_transaction ON RDV.h_transaction (transaction_id);

--СОЗДАНИЕ СВЯЗИ СЧЕТ-ТРАНЗАКЦИЯ
CREATE TABLE RDV.l_account_transaction (
    account_txn_hash_key    VARCHAR2(64) PRIMARY KEY, -- Хэш от всех ключей в ссылке
    debit_account_hash_key  VARCHAR2(64) NOT NULL, -- Ссылка на H_ACCOUNT (счет дебета)
    credit_account_hash_key VARCHAR2(64) NOT NULL, -- Ссылка на H_ACCOUNT (счет кредита)
    transaction_hash_key    VARCHAR2(64) NOT NULL, -- Ссылка на H_TRANSACTION
    load_date               DATE NOT NULL,
    record_source           VARCHAR2(50) NOT NULL,
    
    -- Внешние ключи на хабы
    FOREIGN KEY (debit_account_hash_key) REFERENCES RDV.h_account(account_hash_key),
    FOREIGN KEY (credit_account_hash_key) REFERENCES RDV.h_account(account_hash_key),
    FOREIGN KEY (transaction_hash_key) REFERENCES RDV.h_transaction(transaction_hash_key)
);
-- Уникальный индекс на комбинацию ключей
CREATE UNIQUE INDEX RDV.bk_l_acc_txn ON RDV.l_account_transaction (debit_account_hash_key, credit_account_hash_key, transaction_hash_key);

-- СОЗДАНИЕ САТЕЛЛИТА К ХАБУ СЧЕТОВ
CREATE TABLE RDV.s_account (
    account_hash_key    VARCHAR2(64) NOT NULL, -- Ссылка на хаб H_ACCOUNT
    load_date           DATE NOT NULL,
    hash_diff           VARCHAR2(64) NOT NULL, -- Хэш всех атрибутов в этой версии
    load_end_date       DATE,                  -- Конец действия версии (техника SCD2)
    account_name        VARCHAR2(50),
    start_balance       NUMBER(10,2),
    record_source       VARCHAR2(50) NOT NULL,
    
    PRIMARY KEY (account_hash_key, load_date),
    FOREIGN KEY (account_hash_key) REFERENCES RDV.h_account(account_hash_key)
);

CREATE TABLE RDV.s_account_transaction (
    account_txn_hash_key    VARCHAR2(64) NOT NULL, -- Ссылка на связь L_ACCOUNT_TRANSACTION
    load_date               DATE NOT NULL,
    hash_diff               VARCHAR2(64) NOT NULL,
    load_end_date           DATE,
    transaction_date        DATE NOT NULL,
    amount                  NUMBER(10,2) NOT NULL,
    record_source           VARCHAR2(50) NOT NULL,
    
    PRIMARY KEY (account_txn_hash_key, load_date),
    FOREIGN KEY (account_txn_hash_key) REFERENCES RDV.l_account_transaction(account_txn_hash_key)
);

-- Связь "Счет-Счет" для иерархии
CREATE TABLE RDV.l_account_account (
    account_parent_child_hash_key VARCHAR2(64) PRIMARY KEY,
    parent_account_hash_key       VARCHAR2(64) NOT NULL, -- Ссылка на хаб H_ACCOUNT (родитель)
    child_account_hash_key        VARCHAR2(64) NOT NULL, -- Ссылка на хаб H_ACCOUNT (потомок)
    load_date                     DATE NOT NULL,
    record_source                 VARCHAR2(50) NOT NULL,
    
    FOREIGN KEY (parent_account_hash_key) REFERENCES RDV.h_account(account_hash_key),
    FOREIGN KEY (child_account_hash_key) REFERENCES RDV.h_account(account_hash_key)
);
CREATE UNIQUE INDEX RDV.bk_l_acc_acc ON RDV.l_account_account (parent_account_hash_key, child_account_hash_key);

--Pit-таблица (Point-in-Time) для счетов: PIT_ACCOUNT
--Эта таблица упрощает получение актуального или срезового состояния счета на любую дату, избегая дорогостоящих соединений с самым большим спутником.
CREATE TABLE RDV.pit_account (
    account_hash_key    VARCHAR2(64) NOT NULL,
    snapshot_date       DATE NOT NULL,    -- Дата, на которую нужны данные
    s_account_hash_key  VARCHAR2(64) NOT NULL, -- Ссылка на актуальную версию в S_ACCOUNT на snapshot_date
    load_date           DATE NOT NULL,
    
    PRIMARY KEY (account_hash_key, snapshot_date),
    FOREIGN KEY (account_hash_key) REFERENCES RDV.h_account(account_hash_key)
);
