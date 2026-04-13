-- СОЗДАНИЕ ХАБА СЧЕТОВ
CREATE TABLE RDV2.h_account (
    account_rk          VARCHAR2(64) PRIMARY KEY, -- Хэш от бизнес-ключа (account_id)
    account_id          VARCHAR2(16) NOT NULL,    -- Исходный бизнес-ключ
    load_date           DATE NOT NULL,
    record_source       VARCHAR2(50) NOT NULL
);
CREATE UNIQUE INDEX RDV2.bk_h_account ON RDV2.h_account (account_id); -- Уникальный индекс на бизнес-ключ

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
CREATE TABLE RDV2.l_transaction_account (
    transaction_debacc_credacc_rk   VARCHAR2(64),         -- ХЭШ от связки ключей
    transaction_rk                  VARCHAR2(64),         -- Ссылка на хаб транзакций
    debit_account_rk                VARCHAR2(64),         -- Ссылка на хаб счетов
    credit_account_rk               VARCHAR2(64),         -- Ссылка на хаб счетов
    load_date                       DATE,                 -- Дата загрузки записи
    record_source                   VARCHAR2(50),         -- Источник записи
    PRIMARY KEY (transaction_debacc_credacc_rk)    
);

-- СОЗДАНИЕ САТТЕЛИТА ДЛЯ СВЯЗИ ТРАНЗАКЦИИ СО СЧЕТОМ
CREATE TABLE RDV2.s_transaction_account (
    transaction_debacc_credacc_rk   VARCHAR2(64),         -- Ссылка на линк связи транзакций и счетов
    valid_from                      DATE,                 -- Дата начала действия записи
    valid_to                        DATE,                 -- Дата окончания действия записи
    transaction_date                DATE,                 -- Дата транзакции
    amount                          NUMBER(10,2),         -- Сумма транзакции
    valid_flg                       CHAR(1),              -- Флаг валидности
    load_date                       DATE,                 -- Дата загрузки записи
    hash_diff                       VARCHAR2(64) NOT NULL,-- Хэш по ключам transaction_debacc_credacc_rk, transaction_date, amount
    record_source                   VARCHAR2(50),         -- Источник записи
    PRIMARY KEY (transaction_debacc_credacc_rk, valid_from)    
);

drop table RDV2.s_transaction;
-- СОЗДАНИЕ САТТЕЛИТА ДЛЯ ТРАНЗАКЦИЙ
/*
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
*/

-- СОЗДАНИЕ СВЯЗИ "Счет-Счет" ДЛЯ ИЕРАРХИИ (надо пересмотреть)
CREATE TABLE RDV2.l_account_account (
    parent_child_account_rk   VARCHAR2(64) PRIMARY KEY,
    parent_account_rk         VARCHAR2(64) NOT NULL, -- Ссылка на хаб H_ACCOUNT (родитель)
    child_account_rk          VARCHAR2(64) NOT NULL, -- Ссылка на хаб H_ACCOUNT (потомок)
    load_date                 DATE NOT NULL,
    record_source             VARCHAR2(50) NOT NULL
);
CREATE UNIQUE INDEX RDV2.bk_l_acc_acc ON RDV2.l_account_account (child_account_rk);
