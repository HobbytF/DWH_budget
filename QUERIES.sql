-- STG

select * from STG.STG_ACCOUNT;
select * from STG.STG_TRANSACTION;
select max(TRANSACTION_ID) from STG.STG_TRANSACTION;

select the_date, ACCOUNT_ID, trunc(INCLUSIVE_BALANCE,2), trunc(EXACT_BALANCE,2) from STG.ACCOUNT_BALANCE
where account_id = '2.1.'
order by 1;
select * from STG.ACCOUNT_BALANCE_DAILY
where account_id = '2.1.'
order by the_date desc;

-- 2950 - менялся счет транзакции - линк-связь счет-транзакция
-- 3201 - менялась сумма транзации - саттелит связи

-- RDV

select * from rdv.h_account; -- хаб счетов
select * from rdv.s_account; -- доп информация по счетам (начальное сальдо, название)
select * from rdv.l_account_account; -- иерархия счетов
select * from RDV.h_transaction; -- хаб транзакций
select * from RDV.l_account_transaction; -- указание на счета в транзакциях
select * from RDV.s_account_transaction where ACCOUNT_TXN_HASH_KEY in ('78b4913d41f6153540142d58a6ad318e'); -- доп информация по транзакциям (сумма, дата)
select * from RDV.pit_account;

select 
    t.TRANSACTION_HASH_KEY
    , t.TRANSACTION_ID
    , sat.AMOUNT
    , sat.TRANSACTION_DATE
    , adeb.ACCOUNT_HASH_KEY as deb_acc_hash_key
    , adeb.ACCOUNT_ID as deb_acc
    , acr.ACCOUNT_HASH_KEY as cred_acc_hash_key
    , acr.ACCOUNT_ID as cred_acc 
    , sat.LOAD_END_DATE
    , sat.LOAD_DATE
from rdv.h_transaction t
join RDV.L_ACCOUNT_TRANSACTION lat on t.TRANSACTION_HASH_KEY = lat.TRANSACTION_HASH_KEY
join RDV.S_ACCOUNT_TRANSACTION sat on sat.ACCOUNT_TXN_HASH_KEY = lat.ACCOUNT_TXN_HASH_KEY
join rdv.h_account acr on lat.CREDIT_ACCOUNT_HASH_KEY = acr.ACCOUNT_HASH_KEY
join rdv.h_account adeb on lat.DEBIT_ACCOUNT_HASH_KEY = adeb.ACCOUNT_HASH_KEY
where t.TRANSACTION_ID in (2950,3201);

select * from (
  select 
      t.TRANSACTION_HASH_KEY
      , t.TRANSACTION_ID
      , sat.AMOUNT
      , sat.TRANSACTION_DATE
      , adeb.ACCOUNT_HASH_KEY as deb_acc_hash_key
      , adeb.ACCOUNT_ID as deb_acc
      , acr.ACCOUNT_HASH_KEY as cred_acc_hash_key
      , acr.ACCOUNT_ID as cred_acc 
      , sat.LOAD_END_DATE
      , sat.LOAD_DATE
  from rdv.h_transaction t
  join RDV.L_ACCOUNT_TRANSACTION lat on t.TRANSACTION_HASH_KEY = lat.TRANSACTION_HASH_KEY
  join RDV.S_ACCOUNT_TRANSACTION sat on sat.ACCOUNT_TXN_HASH_KEY = lat.ACCOUNT_TXN_HASH_KEY
  join rdv.h_account acr on lat.CREDIT_ACCOUNT_HASH_KEY = acr.ACCOUNT_HASH_KEY
  join rdv.h_account adeb on lat.DEBIT_ACCOUNT_HASH_KEY = adeb.ACCOUNT_HASH_KEY
  where t.TRANSACTION_ID in (2950,3201)
)  bbridge 
where sysdate between load_date and nvl(load_end_date,to_date('31122049','ddmmyyyy'))
and load_date = max(load_date) over (partition by TRANSACTION_HASH_KEY);




truncate table RDV.l_account_transaction;
truncate table RDV.s_account_transaction;
truncate table RDV.h_transaction;
truncate TABLE RDV.pit_account;

-- IDL

select * from IDL.BRIDGE_ACCOUNT_AGGREGATION;
select * from IDL.BRIDGE_ACCOUNT_TRANSACTION;
select * from IDL.BRIDGE_ACCOUNT_HIERARCHY;
select * from IDL.load_tracking;

-- FLOW

SET SERVEROUTPUT ON;
exec flow.cf_stg_rdv_complete_load ('YAST', 30);
exec flow.run_incremental_idl_load;
