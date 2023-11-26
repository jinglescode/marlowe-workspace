DROP VIEW IF EXISTS analysis.contracts_label_training;
DROP VIEW IF EXISTS analysis.contracts;
DROP VIEW IF EXISTS analysis.contracts_status;
DROP VIEW IF EXISTS analysis.marlowe_contracttxouttag;
DROP VIEW IF EXISTS analysis.chain_txout;
DROP VIEW IF EXISTS analysis.marlowe_contracttxout;
DROP VIEW IF EXISTS analysis.marlowe_createtxout;
DROP VIEW IF EXISTS analysis.trend_by_slots;


CREATE OR REPLACE VIEW analysis.chain_txout
 AS
 SELECT txout.txix,
    txout.slotno,
    txout.lovelace,
    txout.iscollateral,
    encode(txout.txid, 'hex'::text) AS txid,
    encode(txout.address, 'hex'::text) AS address,
    encode(txout.datumhash, 'hex'::text) AS datumhash,
    encode(txout.datumbytes, 'hex'::text) AS datumbytes,
    encode(txout.addressheader, 'hex'::text) AS addressheader
   FROM chain.txout;


CREATE OR REPLACE VIEW analysis.marlowe_contracttxout
 AS
 SELECT contracttxout.txix,
    encode(contracttxout.txid, 'hex'::text) AS txid,
    encode(contracttxout.blockid, 'hex'::text) AS blockid,
    encode(contracttxout.payoutscripthash, 'hex'::text) AS payoutscripthash,
    encode(contracttxout.contract, 'hex'::text) AS contract,
    encode(contracttxout.state, 'hex'::text) AS state,
    encode(contracttxout.rolescurrency, 'hex'::text) AS rolescurrency
   FROM marlowe.contracttxout;


CREATE OR REPLACE VIEW analysis.marlowe_createtxout
 AS
 SELECT createtxout.txix,
    createtxout.slotno,
    createtxout.blockno,
    encode(createtxout.txid, 'hex'::text) AS txid,
    encode(createtxout.blockid, 'hex'::text) AS blockid,
    encode(createtxout.metadata, 'hex'::text) AS metadata
   FROM marlowe.createtxout;


CREATE OR REPLACE VIEW analysis.marlowe_contracttxouttag
 AS
 SELECT encode(contracttxouttag.txid, 'hex'::text) AS id,
    contracttxouttag.tag
   FROM marlowe.contracttxouttag;

CREATE OR REPLACE VIEW analysis.contracts_status
AS 
  SELECT encode(t1.txid, 'hex'::text) AS id,
    t1.created,
    t1.closed
   FROM ( SELECT createtxout.txid,
            1 AS created,
            0 AS closed
           FROM marlowe.createtxout
        UNION
         SELECT applytx.txid,
            0,
                CASE
                    WHEN applytx.outputtxix IS NULL THEN 1
                    ELSE 0
                END AS "case"
           FROM marlowe.applytx) t1;


-- CREATE OR REPLACE VIEW analysis.contracts
--  AS
--   SELECT contracts.id,
--     contracts.blockno,
--     contracts.slotno,
--     contracts.hex,
--     tags.tag,
--     status.created,
--     status.closed
--    FROM ( SELECT t1.txid AS id,
--             t4.blockno,
--             t4.slotno,
--             t1.datumbytes AS hex
--            FROM ( SELECT chain_txout.txid,
--                     chain_txout.datumbytes
--                    FROM analysis.chain_txout
--                   WHERE chain_txout.datumbytes IS NOT NULL) t1
--              JOIN ( SELECT t2.txid,
--                     t3.blockno,
--                     t3.slotno
--                    FROM ( SELECT marlowe_contracttxout.txid
--                            FROM analysis.marlowe_contracttxout) t2
--                      JOIN ( SELECT marlowe_createtxout.txid,
--                             marlowe_createtxout.blockno,
--                             marlowe_createtxout.slotno
--                            FROM analysis.marlowe_createtxout) t3 ON t2.txid = t3.txid) t4 ON t1.txid = t4.txid) contracts
--      LEFT JOIN ( SELECT marlowe_contracttxouttag.id,
--             marlowe_contracttxouttag.tag
--            FROM analysis.marlowe_contracttxouttag) tags ON contracts.id = tags.id
--      LEFT JOIN ( SELECT contracts_status.id,
--             contracts_status.created,
--             contracts_status.closed
--            FROM analysis.contracts_status) status ON contracts.id = status.id;


CREATE OR REPLACE VIEW analysis.contracts
 AS
 SELECT encode(contracts.txid, 'hex'::text) AS id,
    contracts.blockno,
    contracts.slotno,
    tags.tag,
    encode(contracts.hex, 'hex'::text) AS hex
   FROM ( SELECT t1.txid,
            t4.blockno,
            t4.slotno,
            t1.datumbytes AS hex
           FROM ( SELECT txout.txid,
                    txout.datumbytes
                   FROM chain.txout
                  WHERE txout.datumbytes IS NOT NULL) t1
             JOIN ( SELECT t2.txid,
                    t3.blockno,
                    t3.slotno
                   FROM ( SELECT contracttxout.txid
                           FROM marlowe.contracttxout) t2
                     JOIN ( SELECT createtxout.txid,
                            createtxout.blockno,
                            createtxout.slotno
                           FROM marlowe.createtxout) t3 ON t2.txid = t3.txid) t4 ON t1.txid = t4.txid) contracts
     LEFT JOIN ( SELECT contracttxouttag.txid,
            contracttxouttag.tag
           FROM marlowe.contracttxouttag) tags ON contracts.txid = tags.txid;


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
        UNION
         SELECT applytx.txid,
            0,
                CASE
                    WHEN applytx.outputtxix IS NULL THEN 1
                    ELSE 0
                END AS "case"
           FROM marlowe.applytx) txs
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
                          GROUP BY txout_1.addressstakeaddressreference) u) v
          GROUP BY v.slotno) x USING (slotno)
  ORDER BY txout.slotno;


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

