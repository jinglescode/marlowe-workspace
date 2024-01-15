#!/usr/bin/env nix-shell
#!nix-shell -i Rscript -p "rWrapper.override{packages = [ rPackages.RPostgreSQL rPackages.data_table rPackages.lubridate rPackages.jsonlite rPackages.ggplot2 rPackages.scales];}"

require(RPostgreSQL)
require(data.table)
require(lubridate)
require(jsonlite)
require(ggplot2)
require(scales)

drv <- dbDriver("PostgreSQL")

networks = c("mainnet", "preprod", "preview")

marloweStat <- function(network)
  data.table(Network=network, dbGetQuery(conns[[network]], "
select distinct
    slotno as \"Slot No\"
  , sum(created * case when txout.txix = 0 then 1 else 0 end) over (order by slotno) as \"Creations\"
  , sum(closed * case when txout.txix = 0 then 1 else 0 end) over (order by slotno) as \"Closures\"
  , sum((created - closed) * case when txout.txix = 0 then 1 else 0 end) over (order by slotno) as \"Active\"
  , count(txs.txid) over (order by slotno) as \"Transaction\"
  , sum(txout.lovelace * case when txout.txix = 0 then 0 else 1 end) over (order by slotno) / 1000000 \"Ada Transacted\"
  , addresses as \"Payment Addresses\"
  , stakes as \"Stake Addresses\"
  from (
    select createtxout.txid, 1 as created, 0 as closed
      from marlowe.createtxout
      inner join chain.txout
        on createtxout.txid = txout.txid
      where addresspaymentcredential not in (select * from veto_payment)
        and addressstakeaddressreference not in (select * from veto_stake)
    union
    select applytx.txid, 0, case when applytx.outputtxix is null then 1 else 0 end
      from marlowe.applytx
      inner join marlowe.createtxout
        on (createtxout.txid, createtxout.txix) = (createtxid, createtxix)
      inner join chain.txout
        on createtxout.txid = txout.txid
      where addresspaymentcredential not in (select * from veto_payment)
        and addressstakeaddressreference not in (select * from veto_stake)
  ) txs
  inner join chain.txout
    using (txid)
  left outer join (
    select slotno, max(addresses) as addresses
    from (
      select slotno, row_number() over (order by slotno) as addresses
        from (
          select min(slotno) as slotno
            from (
              select txid
                from marlowe.createtxout
              union
              select txid
                from marlowe.applytx
            ) txs
            inner join chain.txout
              using (txid)
            where addresspaymentcredential not in (select * from veto_payment)
              and addressstakeaddressreference not in (select * from veto_stake)
            group by txout.addresspaymentcredential
        ) u
    ) v
    group by slotno
  ) w
    using (slotno)
  left outer join (
    select slotno, max(stakes) as stakes
    from (
      select slotno, row_number() over (order by slotno) as stakes
        from (
          select min(slotno) as slotno
            from (
              select txid
                from marlowe.createtxout
              union
              select txid
                from marlowe.applytx
            ) txs
            inner join chain.txout
              using (txid)
            where addresspaymentcredential not in (select * from veto_payment)
              and addressstakeaddressreference not in (select * from veto_stake)
            group by addressstakeaddressreference
        ) u
    ) v
    group by slotno
  ) x
    using (slotno)
order by 1
")
)

offsets <- list(
    mainnet = 1685297070 - 93730779,
    preprod = 1685297096 - 29613896,
    preview = 1685297218 - 18641218
)

to_epoch <- function(network, slotno) {
    slotno + offsets[[network]]
}


dbnames <- list(
  mainnet="mainnet",
  preprod="preprod",
  preview="preview"
)

conns <- lapply(networks, function(network) dbConnect(
    drv,
    host = "192.168.0.12",
    user = "cardano",
    password = "bcb33b5c09e31e3dd5a2b4ff0ee111e6",
    dbname = dbnames[[network]]
))
names(conns) <- networks

df <- rbind(
    marloweStat("mainnet"),
    marloweStat("preprod"),
    marloweStat("preview")
)

df[,
   `Timestamp` := mapply(to_epoch, `Network`, `Slot No`)
]

ggplot(df, aes(x=as_datetime(`Timestamp`), y=`Creations`, color=`Network`)) +
    geom_line() +
    geom_label(
        aes(label = `Creations`),
        data = df[, .(`Timestamp`=max(`Timestamp`), `Creations`=last(`Creations`)), by=.(`Network`)],
        hjust=0, show.legend = FALSE
    ) +
    scale_x_datetime(label=date_format(), expand = expand_scale(mult = c(0.05, 0.20))) +
    scale_y_continuous(label=label_number_auto()) +
    xlab("Date") +
    ylab("Marlowe Contracts") +
    theme(axis.text.x = element_text(angle = 0, vjust = 0.5, hjust=0.5))

ggsave("contracts.png", width=7, height=5, units="in", dpi=200)

ggplot(df, aes(x=as_datetime(`Timestamp`), y=`Active`, color=`Network`)) +
    geom_line() +
    geom_label(
        aes(label = `Active`),
        data = df[, .(`Timestamp`=max(`Timestamp`), `Active`=last(`Active`)), by=.(`Network`)],
        hjust=0, show.legend = FALSE
    ) +
    scale_x_datetime(label=date_format(), expand = expand_scale(mult = c(0.05, 0.20))) +
    scale_y_continuous(label=label_number_auto()) +
    xlab("Date") +
    ylab("Open Marlowe Contracts") +
    theme(axis.text.x = element_text(angle = 0, vjust = 0.5, hjust=0.5))

ggsave("open.png", width=7, height=5, units="in", dpi=200)

ggplot(df, aes(x=as_datetime(`Timestamp`), y=`Transaction`, color=`Network`)) +
    geom_line() +
    geom_label(
        aes(label = `Transaction`),
        data = df[, .(`Timestamp`=max(`Timestamp`), `Transaction`=last(`Transaction`)), by=.(`Network`)],
        hjust=0, show.legend = FALSE
    ) +
    scale_x_datetime(label=date_format(), expand = expand_scale(mult = c(0.05, 0.20))) +
    scale_y_continuous(label=label_number_auto()) +
    xlab("Date") +
    ylab("Marlowe Transactions") +
    theme(axis.text.x = element_text(angle = 0, vjust = 0.5, hjust=0.5))

ggsave("transactions.png", width=7, height=5, units="in", dpi=200)

ggplot(df, aes(x=as_datetime(`Timestamp`), y=`Ada Transacted`, color=`Network`)) +
    geom_line() +
    geom_label(
        aes(label = `Ada Transacted`),
        data = df[, .(`Timestamp`=max(`Timestamp`), `Ada Transacted`=round(last(`Ada Transacted`))), by=.(`Network`)],
        hjust=0, show.legend = FALSE
    ) +
    scale_x_datetime(label=date_format(), expand = expand_scale(mult = c(0.05, 0.20))) +
    scale_y_continuous(label=label_number_auto()) +
    xlab("Date") +
    ylab("Ada Transacted by Marlowe") +
    theme(axis.text.x = element_text(angle = 0, vjust = 0.5, hjust=0.5))

ggsave("ada.png", width=7, height=5, units="in", dpi=200)

ggplot(df[!is.na(`Payment Addresses`)], aes(x=as_datetime(`Timestamp`), y=`Payment Addresses`, color=`Network`)) +
    geom_line() +
    geom_label(
        aes(label = `Payment Addresses`),
        data = df[!is.na(`Payment Addresses`), .(`Timestamp`=max(`Timestamp`), `Payment Addresses`=round(last(`Payment Addresses`))), by=.(`Network`)],
        hjust=0, show.legend = FALSE
    ) +
    scale_x_datetime(label=date_format(), expand = expand_scale(mult = c(0.05, 0.20))) +
    scale_y_continuous(label=label_number_auto()) +
    xlab("Date") +
    ylab("Unique Payment Addresses in Marlowe Transactions") +
    theme(axis.text.x = element_text(angle = 0, vjust = 0.5, hjust=0.5))

ggsave("payments.png", width=7, height=5, units="in", dpi=200)

ggplot(df[!is.na(`Stake Addresses`)], aes(x=as_datetime(`Timestamp`), y=`Stake Addresses`, color=`Network`)) +
    geom_line() +
    geom_label(
        aes(label = `Stake Addresses`),
        data = df[!is.na(`Stake Addresses`), .(`Timestamp`=max(`Timestamp`), `Stake Addresses`=round(last(`Stake Addresses`))), by=.(`Network`)],
        hjust=0, show.legend = FALSE
    ) +
    scale_x_datetime(label=date_format(), expand = expand_scale(mult = c(0.05, 0.20))) +
    scale_y_continuous(label=label_number_auto()) +
    xlab("Date") +
    ylab("Unique Stake Addresses in Marlowe Transactions") +
    theme(axis.text.x = element_text(angle = 0, vjust = 0.5, hjust=0.5))

ggsave("stakes.png", width=7, height=5, units="in", dpi=200)

lapply(conns, dbDisconnect)

dbUnloadDriver(drv)

f <- file("latest.json")
writeLines(toJSON(
    df[,
        .(
            `Slot No`=last(`Slot No`),
            `Creations`=last(`Creations`),
            `Closures`=last(`Closures`),
            `Actives`=last(`Active`),
            `Transactions`=last(`Transaction`),
            `Ada Transacted`=last(`Ada Transacted`),
            `Payment Addresses`=last(na.omit(`Payment Addresses`)),
            `Stake Addresses`=last(na.omit(`Stake Addresses`))
        ),
        by=`Network`
    ]
), f)
close(f)

system("ipfs pin remote rm --service=pinata --name=marlowestat-ext --force")
system("ipfs add --quieter --pin=false --recursive=true . > marlowe-ext.cid")
system("ipfs pin remote add --service=pinata --name=marlowestat-ext $(cat marlowe-ext.cid)")
system("ipfs name publish --key=marlowestat-ext --lifetime=12m $(cat marlowe-ext.cid)")

