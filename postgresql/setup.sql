-- DROP VIEWS

DROP VIEW IF EXISTS analysis.contracts_merged;
DROP VIEW IF EXISTS analysis.contracts;

DROP VIEW IF EXISTS analysis.contracts_status;
DROP VIEW IF EXISTS analysis.contracts_runner;
DROP VIEW IF EXISTS analysis.contracts_tags;
DROP VIEW IF EXISTS analysis.contracts_txout;
DROP VIEW IF EXISTS analysis.contracts_stakeaddress;

DROP VIEW IF EXISTS analysis.trend_by_slots;


-- CREATE TABLES

-- analysis.blacklist_payment contains a list of addresses that are to be removed from analysis
CREATE TABLE IF NOT EXISTS analysis.blacklist_payment
(
    addresspaymentcredential bytea
)
TABLESPACE pg_default;

-- analysis.blacklist_stake contains a list of addresses that are to be removed from analysis
CREATE TABLE IF NOT EXISTS analysis.blacklist_stake
(
    addressstakeaddressreference bytea
)
TABLESPACE pg_default;


-- CREATE VIEWS

-- analysis.contracts_status checks if a contract is closed or not
CREATE OR REPLACE VIEW analysis.contracts_status
AS 
SELECT
  createTxOut.txId,
  createTxOut.txIx,
  CASE
    WHEN closeTx.txId IS NULL THEN 'active'
    ELSE 'closed'
  END as status
FROM marlowe.createTxOut
LEFT JOIN marlowe.applytx as closeTx
  ON closeTx.createTxId = createTxOut.txId
  AND closeTx.createTxIx = createTxOut.txIx
  AND closeTx.outputTxIx IS NULL;

-- analysis.contracts_size checks the size of a contract
CREATE OR REPLACE VIEW analysis.contracts_size
 AS
 SELECT createtxout.txid,
    createtxout.txix,
    length(contracttxout.contract) AS size
   FROM marlowe.createtxout
     JOIN marlowe.contracttxout USING (txid, txix, blockid);

-- analysis.contracts_tags get the tags of a contract
-- note some contracts have multiple tags
CREATE OR REPLACE VIEW analysis.contracts_tags
 AS
 SELECT contracttxouttag.txid,
    contracttxouttag.tag
   FROM marlowe.contracttxouttag;

-- are contracts created on runner
CREATE OR REPLACE VIEW analysis.contracts_runner
 AS
 SELECT DISTINCT main.txid
   FROM ( SELECT contracts_tags.txid
           FROM analysis.contracts_tags
          WHERE contracts_tags.tag = 'run-lite'::text) main;

-- analysis.contracts_txout gets the total ada transacted and number of transactions of a contract
CREATE OR REPLACE VIEW analysis.contracts_txout
 AS
 SELECT t1.txid,
    sum(t1.lovelace) / 1000000::numeric AS ada_transacted,
    count(t1.txix) AS num_transactions
   FROM ( SELECT DISTINCT txout.txid,
            txout.lovelace,
            txout.txix
           FROM chain.txout) t1
  GROUP BY t1.txid;

-- analysis.contracts_stakeaddress gets the stake address
CREATE OR REPLACE VIEW analysis.contracts_stakeaddress
 AS
 SELECT DISTINCT txout.txid,
    txout.addressstakeaddressreference
   FROM ( SELECT txout_1.txid,
            txout_1.addressstakeaddressreference
           FROM chain.txout txout_1
          WHERE txout_1.addressstakeaddressreference IS NOT NULL) txout;


-- analysis.contracts is a list of unique contracts created
CREATE OR REPLACE VIEW analysis.contracts
 AS
 SELECT contracts_inner.txid,
    max(contracts_inner.created) AS created,
    max(contracts_inner.closed) AS closed,
    min(contracts_inner.blockno) AS blockno,
    min(contracts_inner.slotno) AS slotno
   FROM ( SELECT createtxout.txid,
            createtxout.blockno,
            createtxout.slotno,
            1 AS created,
            0 AS closed
           FROM ( SELECT createtxout_1.txid,
                    createtxout_1.txix,
                    createtxout_1.blockno,
                    createtxout_1.slotno
                   FROM marlowe.createtxout createtxout_1) createtxout
             JOIN chain.txout txout_1 ON createtxout.txid = txout_1.txid
          WHERE NOT (txout_1.addresspaymentcredential IN ( SELECT blacklist_payment.addresspaymentcredential
                   FROM analysis.blacklist_payment)) AND NOT (txout_1.addressstakeaddressreference IN ( SELECT blacklist_stake.addressstakeaddressreference
                   FROM analysis.blacklist_stake))
        UNION
         SELECT createtxout.txid,
            createtxout.blockno,
            createtxout.slotno,
            0 AS created,
                CASE
                    WHEN applytx.outputtxix IS NULL THEN 1
                    ELSE 0
                END AS closed
           FROM marlowe.applytx
             JOIN ( SELECT createtxout_1.txid,
                    createtxout_1.txix,
                    createtxout_1.blockno,
                    createtxout_1.slotno
                   FROM marlowe.createtxout createtxout_1) createtxout ON createtxout.txid = applytx.createtxid AND createtxout.txix = applytx.createtxix
             JOIN chain.txout txout_1 ON createtxout.txid = txout_1.txid
          WHERE NOT (txout_1.addresspaymentcredential IN ( SELECT blacklist_payment.addresspaymentcredential
                   FROM analysis.blacklist_payment)) AND NOT (txout_1.addressstakeaddressreference IN ( SELECT blacklist_stake.addressstakeaddressreference
                   FROM analysis.blacklist_stake))) contracts_inner
  GROUP BY contracts_inner.txid;

-- analysis.contracts_merged is a list of unique contracts created with its related information
CREATE OR REPLACE VIEW analysis.contracts_merged
 AS
   SELECT encode(contracts.txid, 'hex'::text) AS id,
    contracts.blockno,
    contracts.slotno,
    contracts.created,
    contracts.closed,
    status.status,
    size.size,
    txout.ada_transacted,
    txout.num_transactions,
    encode(contracts_stakeaddress.addressstakeaddressreference, 'hex'::text) AS stakeaddress,
        CASE
            WHEN runner.txid IS NULL THEN 0
            ELSE 1
        END AS is_runner
   FROM ( SELECT contracts_1.txid,
            contracts_1.created,
            contracts_1.closed,
            contracts_1.blockno,
            contracts_1.slotno
           FROM analysis.contracts contracts_1) contracts
     LEFT JOIN ( SELECT contracts_status.txid,
            contracts_status.status
           FROM analysis.contracts_status) status ON contracts.txid = status.txid
     LEFT JOIN ( SELECT contracts_size.txid,
            contracts_size.size
           FROM analysis.contracts_size) size ON contracts.txid = size.txid
     LEFT JOIN ( SELECT contracts_txout.txid,
            contracts_txout.ada_transacted,
            contracts_txout.num_transactions
           FROM analysis.contracts_txout) txout ON contracts.txid = txout.txid
     LEFT JOIN ( SELECT contracts_stakeaddress_1.txid,
            contracts_stakeaddress_1.addressstakeaddressreference
           FROM analysis.contracts_stakeaddress contracts_stakeaddress_1) contracts_stakeaddress ON contracts.txid = contracts_stakeaddress.txid
     LEFT JOIN ( SELECT contracts_runner.txid
           FROM analysis.contracts_runner) runner ON contracts.txid = runner.txid;

-- this is old analysis.contracts_merged, has been replace with above that filter out contracts in blacklists
--     SELECT encode(contracts.txid, 'hex'::text) AS id,
--     contracts.blockno,
--     contracts.slotno,
--     contracts.addresspaymentcredential,
--     contracts.addressstakeaddressreference,
--     status.status,
--     size.size,
--     encode(contracts.hex, 'hex'::text) AS hex
--    FROM ( SELECT t1.txid,
--             t1.addresspaymentcredential,
--             t1.addressstakeaddressreference,
--             t4.blockno,
--             t4.slotno,
--             t1.datumbytes AS hex
--            FROM ( SELECT txout.txid,
--                     txout.datumbytes,
--                     txout.addresspaymentcredential,
--                     txout.addressstakeaddressreference
--                    FROM chain.txout
--                   WHERE txout.datumbytes IS NOT NULL) t1
--              JOIN ( SELECT t2.txid,
--                     t3.blockno,
--                     t3.slotno
--                    FROM ( SELECT contracttxout.txid
--                            FROM marlowe.contracttxout) t2
--                      JOIN ( SELECT createtxout.txid,
--                             createtxout.blockno,
--                             createtxout.slotno
--                            FROM marlowe.createtxout) t3 ON t2.txid = t3.txid) t4 ON t1.txid = t4.txid) contracts
--      LEFT JOIN ( SELECT contracts_status.txid,
--             contracts_status.status
--            FROM analysis.contracts_status) status ON contracts.txid = status.txid
--      LEFT JOIN ( SELECT contracts_size.txid,
--             contracts_size.size
--            FROM analysis.contracts_size) size ON contracts.txid = size.txid;

-- analysis.trend_by_slots is the stats from brian
CREATE OR REPLACE VIEW analysis.trend_by_slots
 AS
  SELECT DISTINCT txout.slotno,
    sum(txs.created *
        CASE
            WHEN txout.txix = 0 THEN 1
            ELSE 0
        END) OVER (ORDER BY txout.slotno) AS creations,
    sum(txs.closed *
        CASE
            WHEN txout.txix = 0 THEN 1
            ELSE 0
        END) OVER (ORDER BY txout.slotno) AS closures,
    sum((txs.created - txs.closed) *
        CASE
            WHEN txout.txix = 0 THEN 1
            ELSE 0
        END) OVER (ORDER BY txout.slotno) AS active,
    count(txs.txid) OVER (ORDER BY txout.slotno) AS transactions,
    sum(txout.lovelace *
        CASE
            WHEN txout.txix = 0 THEN 0
            ELSE 1
        END) OVER (ORDER BY txout.slotno) / 1000000::numeric AS ada_transacted,
    w.addresses AS payment_addresses,
    x.stakes AS stake_addresses
   FROM ( SELECT createtxout.txid,
            1 AS created,
            0 AS closed
           FROM marlowe.createtxout
             JOIN chain.txout txout_1 ON createtxout.txid = txout_1.txid
          WHERE NOT (txout_1.addresspaymentcredential IN ( SELECT blacklist_payment.addresspaymentcredential
                   FROM analysis.blacklist_payment)) AND NOT (txout_1.addressstakeaddressreference IN ( SELECT blacklist_stake.addressstakeaddressreference
                   FROM analysis.blacklist_stake))
        UNION
         SELECT applytx.txid,
            0,
                CASE
                    WHEN applytx.outputtxix IS NULL THEN 1
                    ELSE 0
                END AS "case"
           FROM marlowe.applytx
             JOIN marlowe.createtxout ON createtxout.txid = applytx.createtxid AND createtxout.txix = applytx.createtxix
             JOIN chain.txout txout_1 ON createtxout.txid = txout_1.txid
          WHERE NOT (txout_1.addresspaymentcredential IN ( SELECT blacklist_payment.addresspaymentcredential
                   FROM analysis.blacklist_payment)) AND NOT (txout_1.addressstakeaddressreference IN ( SELECT blacklist_stake.addressstakeaddressreference
                   FROM analysis.blacklist_stake))) txs
     JOIN chain.txout USING (txid)
     LEFT JOIN ( SELECT v.slotno,
            max(v.addresses) AS addresses
           FROM ( SELECT u.slotno,
                    row_number() OVER (ORDER BY u.slotno) AS addresses
                   FROM ( SELECT min(txout_1.slotno) AS slotno
                           FROM ( SELECT createtxout.txid
                                   FROM marlowe.createtxout
                                UNION
                                 SELECT applytx.txid
                                   FROM marlowe.applytx) txs_1
                             JOIN chain.txout txout_1 USING (txid)
                          WHERE NOT (txout_1.addresspaymentcredential IN ( SELECT blacklist_payment.addresspaymentcredential
                                   FROM analysis.blacklist_payment)) AND NOT (txout_1.addressstakeaddressreference IN ( SELECT blacklist_stake.addressstakeaddressreference
                                   FROM analysis.blacklist_stake))
                          GROUP BY txout_1.addresspaymentcredential) u) v
          GROUP BY v.slotno) w USING (slotno)
     LEFT JOIN ( SELECT v.slotno,
            max(v.stakes) AS stakes
           FROM ( SELECT u.slotno,
                    row_number() OVER (ORDER BY u.slotno) AS stakes
                   FROM ( SELECT min(txout_1.slotno) AS slotno
                           FROM ( SELECT createtxout.txid
                                   FROM marlowe.createtxout
                                UNION
                                 SELECT applytx.txid
                                   FROM marlowe.applytx) txs_1
                             JOIN chain.txout txout_1 USING (txid)
                          WHERE NOT (txout_1.addresspaymentcredential IN ( SELECT blacklist_payment.addresspaymentcredential
                                   FROM analysis.blacklist_payment)) AND NOT (txout_1.addressstakeaddressreference IN ( SELECT blacklist_stake.addressstakeaddressreference
                                   FROM analysis.blacklist_stake))
                          GROUP BY txout_1.addressstakeaddressreference) u) v
          GROUP BY v.slotno) x USING (slotno)
  ORDER BY txout.slotno;

-- this is old analysis.trend_by_slots, has been replace with above that filter out contracts in blacklists
--  SELECT DISTINCT txout.slotno,
--     sum(txs.created *
--         CASE
--             WHEN txout.txix = 0 THEN 1
--             ELSE 0
--         END) OVER (ORDER BY txout.slotno) AS creations,
--     sum(txs.closed *
--         CASE
--             WHEN txout.txix = 0 THEN 1
--             ELSE 0
--         END) OVER (ORDER BY txout.slotno) AS closures,
--     sum((txs.created - txs.closed) *
--         CASE
--             WHEN txout.txix = 0 THEN 1
--             ELSE 0
--         END) OVER (ORDER BY txout.slotno) AS active,
--     count(txs.txid) OVER (ORDER BY txout.slotno) AS transactions,
--     sum(txout.lovelace *
--         CASE
--             WHEN txout.txix = 0 THEN 0
--             ELSE 1
--         END) OVER (ORDER BY txout.slotno) / 1000000::numeric AS ada_transacted,
--     w.addresses AS payment_addresses,
--     x.stakes AS stake_addresses
--    FROM ( SELECT createtxout.txid,
--             1 AS created,
--             0 AS closed
--            FROM marlowe.createtxout
--         UNION
--          SELECT applytx.txid,
--             0,
--                 CASE
--                     WHEN applytx.outputtxix IS NULL THEN 1
--                     ELSE 0
--                 END AS "case"
--            FROM marlowe.applytx) txs
--      JOIN chain.txout USING (txid)
--      LEFT JOIN ( SELECT v.slotno,
--             max(v.addresses) AS addresses
--            FROM ( SELECT u.slotno,
--                     row_number() OVER (ORDER BY u.slotno) AS addresses
--                    FROM ( SELECT min(txout_1.slotno) AS slotno
--                            FROM ( SELECT createtxout.txid
--                                    FROM marlowe.createtxout
--                                 UNION
--                                  SELECT applytx.txid
--                                    FROM marlowe.applytx) txs_1
--                              JOIN chain.txout txout_1 USING (txid)
--                           GROUP BY txout_1.addresspaymentcredential) u) v
--           GROUP BY v.slotno) w USING (slotno)
--      LEFT JOIN ( SELECT v.slotno,
--             max(v.stakes) AS stakes
--            FROM ( SELECT u.slotno,
--                     row_number() OVER (ORDER BY u.slotno) AS stakes
--                    FROM ( SELECT min(txout_1.slotno) AS slotno
--                            FROM ( SELECT createtxout.txid
--                                    FROM marlowe.createtxout
--                                 UNION
--                                  SELECT applytx.txid
--                                    FROM marlowe.applytx) txs_1
--                              JOIN chain.txout txout_1 USING (txid)
--                           GROUP BY txout_1.addressstakeaddressreference) u) v
--           GROUP BY v.slotno) x USING (slotno)
--   ORDER BY txout.slotno;


-- CREATE OR REPLACE VIEW analysis.contracts_label_training
--  AS
--  SELECT contracts.id,
--     contracts.hex,
--     labels.label
--    FROM ( SELECT contracts_1.id,
--             contracts_1.blockno,
--             contracts_1.slotno,
--             contracts_1.hex
--            FROM analysis.contracts contracts_1
--           WHERE (contracts_1.id IN ( SELECT contracts_label.id
--                    FROM analysis.contracts_label))) contracts
--      JOIN ( SELECT contracts_label.id,
--             contracts_label.label
--            FROM analysis.contracts_label) labels ON contracts.id = labels.id;

