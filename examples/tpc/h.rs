//! TPC-H: `q` schema, a columnar data generator in the spirit of `dbgen`, the 22 queries in `q` / `qSQL`, and checks
//! of several answers against values computed independently from the generated rows.
//!
//! The generator follows the specification's cardinalities, key relationships and value domains, but it is not
//! `dbgen`: its random streams differ, so the answers are not the published validation answers. Correctness is
//! therefore checked by recomputing selected queries in Rust from the same rows.
#![allow(dead_code)]
use crate::util::*;
use chrono::{Duration, NaiveDate};
use kixdb::Connection;
use std::collections::HashMap;

pub const TABLES: &[&str] = &["region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"];

pub const DISTRIBUTE: &[&str] = &[
    "DISTRIBUTE region REPLICATED",
    "DISTRIBUTE nation REPLICATED",
    "DISTRIBUTE supplier BY s_suppkey",
    "DISTRIBUTE part BY p_partkey",
    "DISTRIBUTE partsupp BY ps_partkey",
    "DISTRIBUTE customer BY c_custkey",
    "DISTRIBUTE orders BY o_orderkey",
    "DISTRIBUTE lineitem BY l_orderkey",
];

pub const SCHEMA: &str = "
region: ([r_regionkey: `long$()] r_name: `$(); r_comment: ());
nation: ([n_nationkey: `long$()] n_name: `$(); n_regionkey: `long$(); n_comment: ());
supplier: ([s_suppkey: `long$()] s_name: `$(); s_address: (); s_nationkey: `long$(); s_phone: (); s_acctbal: `float$(); s_comment: ());
part: ([p_partkey: `long$()] p_name: (); p_mfgr: `$(); p_brand: `$(); p_type: `$(); p_size: `long$(); p_container: `$(); p_retailprice: `float$(); p_comment: ());
partsupp: ([] ps_partkey: `long$(); ps_suppkey: `long$(); ps_availqty: `long$(); ps_supplycost: `float$(); ps_comment: ());
customer: ([c_custkey: `long$()] c_name: `$(); c_address: (); c_nationkey: `long$(); c_phone: (); c_acctbal: `float$(); c_mktsegment: `$(); c_comment: ());
orders: ([o_orderkey: `long$()] o_custkey: `long$(); o_orderstatus: \"c\"$(); o_totalprice: `float$(); o_orderdate: `date$(); o_orderpriority: `$(); o_clerk: `$(); o_shippriority: `long$(); o_comment: ());
lineitem: ([] l_orderkey: `long$(); l_partkey: `long$(); l_suppkey: `long$(); l_linenumber: `long$(); l_quantity: `float$(); l_extendedprice: `float$(); l_discount: `float$(); l_tax: `float$(); l_returnflag: \"c\"$(); l_linestatus: \"c\"$(); l_shipdate: `date$(); l_commitdate: `date$(); l_receiptdate: `date$(); l_shipinstruct: `$(); l_shipmode: `$(); l_comment: ())
";

const NATIONS: [(&str, i64); 25] = [
    ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4), ("ETHIOPIA", 0), ("FRANCE", 3),
    ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2), ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4),
    ("KENYA", 0), ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3), ("SAUDI ARABIA", 4),
    ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3), ("UNITED STATES", 1),
];
const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
const COLORS: [&str; 24] = [
    "almond", "antique", "azure", "beige", "black", "blue", "brown", "burlywood", "chartreuse", "coral", "cornsilk",
    "forest", "frosted", "ghost", "goldenrod", "green", "honeydew", "ivory", "khaki", "lime", "magenta", "navy",
    "olive", "peru",
];
const TYPE1: [&str; 6] = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"];
const TYPE2: [&str; 5] = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"];
const TYPE3: [&str; 5] = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"];
const CONT1: [&str; 5] = ["SM", "LG", "MED", "JUMBO", "WRAP"];
const CONT2: [&str; 8] = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"];
const SEGMENTS: [&str; 5] = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"];
const PRIORITIES: [&str; 5] = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
const INSTRUCT: [&str; 4] = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"];
const MODES: [&str; 7] = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];

/// The generated rows the checks need.
pub struct Data {
    pub sf: f64,
    pub customers: usize,
    lines: Vec<Line>,
    orders: HashMap<i64, Order>,
    promo: HashMap<i64, bool>,
}

struct Order {
    priority: &'static str,
}

struct Line {
    orderkey: i64,
    partkey: i64,
    qty: i64,
    price: i64, // cents
    disc: i64,  // hundredths
    rf: char,
    ls: char,
    ship: NaiveDate,
    commit: NaiveDate,
    receipt: NaiveDate,
    mode: &'static str,
}

fn d(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

fn phone(rng: &mut Rng, nation: i64) -> String {
    format!("{}-{}-{}-{}", nation + 10, rng.digits(3), rng.digits(3), rng.digits(4))
}

fn dec2(whole: i64, cents: i64) -> f64 {
    format!("{whole}.{cents:02}").parse::<f64>().unwrap()
}

pub fn load(c: &mut Connection, sf: f64) -> Data {
    must(c, SCHEMA);
    let mut rng = Rng::new(19_920_101);
    let n_supp = ((10_000.0 * sf) as i64).max(10);
    let n_part = ((200_000.0 * sf) as i64).max(40);
    let n_cust = ((150_000.0 * sf) as i64).max(30);
    let n_ord = ((1_500_000.0 * sf) as i64).max(300);

    let r_comment: Vec<String> = (0..REGIONS.len()).map(|_| rng.text(4)).collect();
    load_cols(c, "region", vec![
        ("r_regionkey", longs((0..REGIONS.len() as i64).collect())),
        ("r_name", syms(REGIONS.to_vec())),
        ("r_comment", strings(r_comment)),
    ]);

    let n_comment: Vec<String> = (0..NATIONS.len()).map(|_| rng.text(5)).collect();
    load_cols(c, "nation", vec![
        ("n_nationkey", longs((0..NATIONS.len() as i64).collect())),
        ("n_name", syms(NATIONS.iter().map(|(n, _)| *n).collect())),
        ("n_regionkey", longs(NATIONS.iter().map(|(_, r)| *r).collect())),
        ("n_comment", strings(n_comment)),
    ]);

    let cap_s = n_supp as usize;
    let (mut s_key, mut s_name, mut s_addr, mut s_nk, mut s_ph, mut s_bal, mut s_cm) =
        (Vec::with_capacity(cap_s), Vec::with_capacity(cap_s), Vec::with_capacity(cap_s), Vec::with_capacity(cap_s), Vec::with_capacity(cap_s), Vec::with_capacity(cap_s), Vec::with_capacity(cap_s));
    for s in 1..=n_supp {
        let nk = rng.int(0, 24);
        let comment = if rng.chance(0.005) { "Customer unhappy Complaints noted".to_string() } else { rng.text(5) };
        let addr = format!("addr {}", rng.digits(8));
        let ph = phone(&mut rng, nk);
        let bal = dec2(rng.int(-999, 9999), rng.int(0, 99));
        s_key.push(s);
        s_name.push(format!("Supplier#{s:09}"));
        s_addr.push(addr);
        s_nk.push(nk);
        s_ph.push(ph);
        s_bal.push(bal);
        s_cm.push(comment);
    }
    load_cols(c, "supplier", vec![
        ("s_suppkey", longs(s_key)), ("s_name", syms(s_name)), ("s_address", strings(s_addr)),
        ("s_nationkey", longs(s_nk)), ("s_phone", strings(s_ph)), ("s_acctbal", floats(s_bal)), ("s_comment", strings(s_cm)),
    ]);

    let cap_p = n_part as usize;
    let mut promo = HashMap::with_capacity(cap_p);
    let mut retail = HashMap::with_capacity(cap_p);
    let (mut p_key, mut p_name, mut p_mfgr, mut p_brand, mut p_type, mut p_size, mut p_cont, mut p_price, mut p_cm) =
        (Vec::with_capacity(cap_p), Vec::with_capacity(cap_p), Vec::with_capacity(cap_p), Vec::with_capacity(cap_p), Vec::with_capacity(cap_p), Vec::with_capacity(cap_p), Vec::with_capacity(cap_p), Vec::with_capacity(cap_p), Vec::with_capacity(cap_p));
    for p in 1..=n_part {
        let name = (0..5).map(|_| *rng.pick(&COLORS)).collect::<Vec<_>>().join(" ");
        let m = rng.int(1, 5);
        let ptype = format!("{} {} {}", rng.pick(&TYPE1), rng.pick(&TYPE2), rng.pick(&TYPE3));
        promo.insert(p, ptype.starts_with("PROMO"));
        let cents = 90_000 + ((p / 10) % 20_001) + 100 * (p % 1000);
        retail.insert(p, cents);
        let brand = format!("Brand#{m}{}", rng.int(1, 5));
        let sz = rng.int(1, 50);
        let cont = format!("{} {}", rng.pick(&CONT1), rng.pick(&CONT2));
        let cm = rng.text(2);
        p_key.push(p);
        p_name.push(name);
        p_mfgr.push(format!("Manufacturer#{m}"));
        p_brand.push(brand);
        p_type.push(ptype);
        p_size.push(sz);
        p_cont.push(cont);
        p_price.push(cents as f64 / 100.0);
        p_cm.push(cm);
    }
    load_cols(c, "part", vec![
        ("p_partkey", longs(p_key)), ("p_name", strings(p_name)), ("p_mfgr", syms(p_mfgr)), ("p_brand", syms(p_brand)),
        ("p_type", syms(p_type)), ("p_size", longs(p_size)), ("p_container", syms(p_cont)), ("p_retailprice", floats(p_price)), ("p_comment", strings(p_cm)),
    ]);

    let supp_of = |p: i64, i: i64| (p + i * (n_supp / 4 + (p - 1) / n_supp)) % n_supp + 1;
    let cap_ps = cap_p * 4;
    let (mut ps_pk, mut ps_sk, mut ps_qty, mut ps_cost, mut ps_cm) =
        (Vec::with_capacity(cap_ps), Vec::with_capacity(cap_ps), Vec::with_capacity(cap_ps), Vec::with_capacity(cap_ps), Vec::with_capacity(cap_ps));
    for p in 1..=n_part {
        for i in 0..4 {
            let qty = rng.int(1, 9999);
            let cost = dec2(rng.int(1, 1000), rng.int(0, 99));
            let cm = rng.text(4);
            ps_pk.push(p);
            ps_sk.push(supp_of(p, i));
            ps_qty.push(qty);
            ps_cost.push(cost);
            ps_cm.push(cm);
        }
    }
    load_cols(c, "partsupp", vec![
        ("ps_partkey", longs(ps_pk)), ("ps_suppkey", longs(ps_sk)), ("ps_availqty", longs(ps_qty)),
        ("ps_supplycost", floats(ps_cost)), ("ps_comment", strings(ps_cm)),
    ]);

    let cap_c = n_cust as usize;
    let (mut c_key, mut c_name, mut c_addr, mut c_nk, mut c_ph, mut c_bal, mut c_seg, mut c_cm) =
        (Vec::with_capacity(cap_c), Vec::with_capacity(cap_c), Vec::with_capacity(cap_c), Vec::with_capacity(cap_c), Vec::with_capacity(cap_c), Vec::with_capacity(cap_c), Vec::with_capacity(cap_c), Vec::with_capacity(cap_c));
    for k in 1..=n_cust {
        let nk = rng.int(0, 24);
        let addr = format!("addr {}", rng.digits(8));
        let ph = phone(&mut rng, nk);
        let bal = dec2(rng.int(-999, 9999), rng.int(0, 99));
        let seg = *rng.pick(&SEGMENTS);
        let cm = rng.text(5);
        c_key.push(k);
        c_name.push(format!("Customer#{k:09}"));
        c_addr.push(addr);
        c_nk.push(nk);
        c_ph.push(ph);
        c_bal.push(bal);
        c_seg.push(seg);
        c_cm.push(cm);
    }
    load_cols(c, "customer", vec![
        ("c_custkey", longs(c_key)), ("c_name", syms(c_name)), ("c_address", strings(c_addr)), ("c_nationkey", longs(c_nk)),
        ("c_phone", strings(c_ph)), ("c_acctbal", floats(c_bal)), ("c_mktsegment", syms(c_seg)), ("c_comment", strings(c_cm)),
    ]);

    // orders and their lines; a third of the customers never order (queries 13 and 22 depend on that)
    let cap_o = n_ord as usize;
    let cap_l = cap_o * 4;
    let (mut o_key, mut o_ck, mut o_st, mut o_tot, mut o_dt, mut o_pri, mut o_clk, mut o_sp, mut o_cm) =
        (Vec::with_capacity(cap_o), Vec::with_capacity(cap_o), Vec::with_capacity(cap_o), Vec::with_capacity(cap_o), Vec::with_capacity(cap_o), Vec::with_capacity(cap_o), Vec::with_capacity(cap_o), Vec::with_capacity(cap_o), Vec::with_capacity(cap_o));
    let (mut l_ok, mut l_pk, mut l_sk, mut l_ln, mut l_qty, mut l_ep, mut l_dc, mut l_tx, mut l_rf, mut l_ls, mut l_sd, mut l_cd, mut l_rd, mut l_si, mut l_sm, mut l_cm) =
        (Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l), Vec::with_capacity(cap_l));

    let mut lines = Vec::with_capacity(cap_l);
    let mut orders = HashMap::with_capacity(cap_o);
    let start = d("1992-01-01");
    let span = (d("1998-08-02") - start).num_days();
    let cutoff = d("1995-06-17");
    for o in 1..=n_ord {
        let mut cust = rng.int(1, n_cust);
        while cust % 3 == 0 {
            cust = rng.int(1, n_cust);
        }
        let odate = start + Duration::days(rng.int(0, span));
        let nlines = rng.int(1, 7);
        let (mut total, mut nf, mut no) = (0f64, 0, 0);
        for ln in 1..=nlines {
            let p = rng.int(1, n_part);
            let s = supp_of(p, rng.int(0, 3));
            let qty = rng.int(1, 50);
            let price = qty * retail[&p];
            let disc = rng.int(0, 10);
            let tax = rng.int(0, 8);
            let ship = odate + Duration::days(rng.int(1, 121));
            let commit = odate + Duration::days(rng.int(30, 90));
            let receipt = ship + Duration::days(rng.int(1, 30));
            let rf = if receipt <= cutoff { if rng.chance(0.5) { 'R' } else { 'A' } } else { 'N' };
            let ls = if ship > cutoff { 'O' } else { 'F' };
            if ls == 'F' { nf += 1 } else { no += 1 }
            total += price as f64 / 100.0 * (1.0 + tax as f64 / 100.0) * (1.0 - disc as f64 / 100.0);
            let mode = *rng.pick(&MODES);
            let inst = *rng.pick(&INSTRUCT);
            let cm = rng.text(3);

            l_ok.push(o);
            l_pk.push(p);
            l_sk.push(s);
            l_ln.push(ln);
            l_qty.push(qty as f64);
            l_ep.push(price as f64 / 100.0);
            l_dc.push(dec2(0, disc));
            l_tx.push(dec2(0, tax));
            l_rf.push(rf as u8);
            l_ls.push(ls as u8);
            l_sd.push(k_date(ship));
            l_cd.push(k_date(commit));
            l_rd.push(k_date(receipt));
            l_si.push(inst);
            l_sm.push(mode);
            l_cm.push(cm);
            lines.push(Line { orderkey: o, partkey: p, qty, price, disc, rf, ls, ship, commit, receipt, mode });
        }
        let status = if no == 0 { b'F' } else if nf == 0 { b'O' } else { b'P' };
        let priority = *rng.pick(&PRIORITIES);
        let comment = if rng.chance(0.02) { "special packages and requests".to_string() } else { rng.text(5) };
        let clerk = format!("Clerk#{:09}", rng.int(1, 1000));
        orders.insert(o, Order { priority });

        o_key.push(o);
        o_ck.push(cust);
        o_st.push(status);
        o_tot.push((total * 100.0).round() / 100.0);
        o_dt.push(k_date(odate));
        o_pri.push(priority);
        o_clk.push(clerk);
        o_sp.push(0);
        o_cm.push(comment);
    }
    load_cols(c, "orders", vec![
        ("o_orderkey", longs(o_key)), ("o_custkey", longs(o_ck)), ("o_orderstatus", chars(o_st)),
        ("o_totalprice", floats(o_tot)), ("o_orderdate", dates(o_dt)), ("o_orderpriority", syms(o_pri)),
        ("o_clerk", syms(o_clk)), ("o_shippriority", longs(o_sp)), ("o_comment", strings(o_cm)),
    ]);
    load_cols(c, "lineitem", vec![
        ("l_orderkey", longs(l_ok)), ("l_partkey", longs(l_pk)), ("l_suppkey", longs(l_sk)), ("l_linenumber", longs(l_ln)),
        ("l_quantity", floats(l_qty)), ("l_extendedprice", floats(l_ep)), ("l_discount", floats(l_dc)), ("l_tax", floats(l_tx)),
        ("l_returnflag", chars(l_rf)), ("l_linestatus", chars(l_ls)), ("l_shipdate", dates(l_sd)), ("l_commitdate", dates(l_cd)),
        ("l_receiptdate", dates(l_rd)), ("l_shipinstruct", syms(l_si)), ("l_shipmode", syms(l_sm)), ("l_comment", strings(l_cm)),
    ]);

    Data { sf, customers: n_cust as usize, lines, orders, promo }
}

/// The 22 queries in `q` / `qSQL`, with the specification's validation parameters.
pub fn queries(sf: f64) -> Vec<(String, String)> {
    let frac = 0.0001 / sf;
    let v: Vec<(&str, String)> = vec![
        (
            "Q1 pricing summary",
            "select sum_qty: sum l_quantity, sum_base_price: sum l_extendedprice, sum_disc_price: sum l_extendedprice * 1 - l_discount, sum_charge: sum (l_extendedprice * 1 - l_discount) * 1 + l_tax, avg_qty: avg l_quantity, avg_price: avg l_extendedprice, avg_disc: avg l_discount, count_order: count i by l_returnflag, l_linestatus from lineitem where l_shipdate <= 1998.12.01 - 90".into(),
        ),
        (
            "Q2 minimum cost supplier",
            "q2_en: exec n_nationkey from nation where n_regionkey in (exec r_regionkey from region where r_name = `EUROPE); \
             q2_sk: exec s_suppkey from supplier where s_nationkey in q2_en; \
             q2_pk: exec p_partkey from part where p_size = 15, p_type like \"*BRASS\"; \
             100 sublist `s_acctbal xdesc `n_name`s_name`p_partkey xasc select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from (((select p_partkey: ps_partkey, s_suppkey: ps_suppkey from partsupp where ps_partkey in q2_pk, ps_suppkey in q2_sk, ps_supplycost = (min; ps_supplycost) fby ps_partkey) lj (select first p_mfgr by p_partkey from part where p_partkey in q2_pk)) lj (select first s_name, first s_address, first s_phone, first s_acctbal, first s_comment, n_nationkey: first s_nationkey by s_suppkey from supplier where s_suppkey in q2_sk)) lj nation".into(),
        ),
        (
            "Q3 shipping priority",
            "q3_ok: exec o_orderkey from orders where o_orderdate < 1995.03.15, o_custkey in (exec c_custkey from customer where c_mktsegment = `BUILDING); \
             10 sublist `revenue xdesc `o_orderdate xasc select l_orderkey, revenue, o_orderdate, o_shippriority from (0! (select revenue: sum l_extendedprice * 1 - l_discount by l_orderkey from lineitem where l_shipdate > 1995.03.15, l_orderkey in q3_ok) ij (select first o_orderdate, first o_shippriority by l_orderkey: o_orderkey from orders where o_orderkey in q3_ok))".into(),
        ),
        (
            "Q4 order priority checking",
            "q4_ok: exec o_orderkey from orders where o_orderdate >= 1993.07.01, o_orderdate < 1993.10.01; \
             q4_late: distinct exec l_orderkey from lineitem where l_orderkey in q4_ok, l_commitdate < l_receiptdate; \
             select order_count: count i by o_orderpriority from orders where o_orderkey in q4_late".into(),
        ),
        (
            "Q5 local supplier volume",
            "q5_an: exec n_nationkey from nation where n_regionkey in (exec r_regionkey from region where r_name = `ASIA); \
             q5_ck: exec c_custkey from customer where c_nationkey in q5_an; \
             q5_ok: exec o_orderkey from orders where o_orderdate >= 1994.01.01, o_orderdate < 1995.01.01, o_custkey in q5_ck; \
             q5_sk: exec s_suppkey from supplier where s_nationkey in q5_an; \
             `revenue xdesc 0! select revenue: sum rev by n_name from (select rev, n_nationkey: s_nationkey from (((select l_orderkey, l_suppkey, rev: l_extendedprice * 1 - l_discount from lineitem where l_orderkey in q5_ok, l_suppkey in q5_sk) lj ((select first o_custkey by l_orderkey: o_orderkey from orders where o_orderkey in q5_ok) lj (select first c_nationkey by o_custkey: c_custkey from customer where c_custkey in q5_ck))) lj (select first s_nationkey by l_suppkey: s_suppkey from supplier where s_suppkey in q5_sk)) where c_nationkey = s_nationkey) lj nation".into(),
        ),
        (
            "Q6 forecasting revenue change",
            "select revenue: sum l_extendedprice * l_discount from lineitem where l_shipdate >= 1994.01.01, l_shipdate < 1995.01.01, l_discount within 0.05 0.07, l_quantity < 24".into(),
        ),
        (
            "Q7 volume shipping",
            "q7_fn: exec n_nationkey from nation where n_name in `FRANCE`GERMANY; \
             q7_sk: exec s_suppkey from supplier where s_nationkey in q7_fn; \
             q7_ck: exec c_custkey from customer where c_nationkey in q7_fn; \
             q7_ok: exec o_orderkey from orders where o_custkey in q7_ck; \
             select revenue: sum volume by supp_nation, cust_nation, l_year from (((select l_orderkey, l_suppkey, l_year: l_shipdate.year, volume: l_extendedprice * 1 - l_discount from lineitem where l_shipdate within 1995.01.01 1996.12.31, l_suppkey in q7_sk, l_orderkey in q7_ok) lj ((select n_nationkey: first s_nationkey by l_suppkey: s_suppkey from supplier where s_suppkey in q7_sk) lj (select supp_nation: first n_name by n_nationkey from nation))) lj ((select first o_custkey by l_orderkey: o_orderkey from orders where o_orderkey in q7_ok) lj ((select n_nationkey: first c_nationkey by o_custkey: c_custkey from customer where c_custkey in q7_ck) lj (select cust_nation: first n_name by n_nationkey from nation)))) where supp_nation <> cust_nation".into(),
        ),
        (
            "Q8 national market share",
            "q8_pk: exec p_partkey from part where p_type = `$\"ECONOMY ANODIZED STEEL\"; \
             q8_an: exec n_nationkey from nation where n_regionkey in (exec r_regionkey from region where r_name = `AMERICA); \
             q8_ck: exec c_custkey from customer where c_nationkey in q8_an; \
             q8_ok: exec o_orderkey from orders where o_orderdate within 1995.01.01 1996.12.31, o_custkey in q8_ck; \
             q8_br: exec s_suppkey from supplier where s_nationkey in (exec n_nationkey from nation where n_name = `BRAZIL); \
             select mkt_share: (sum volume * l_suppkey in q8_br) % sum volume by o_year from (select l_orderkey, l_suppkey, volume: l_extendedprice * 1 - l_discount from lineitem where l_partkey in q8_pk, l_orderkey in q8_ok) lj (select o_year: first o_orderdate.year by l_orderkey: o_orderkey from orders where o_orderkey in q8_ok)".into(),
        ),
        (
            "Q9 product type profit",
            "q9_pk: exec p_partkey from part where p_name like \"*green*\"; \
             `nat xasc `o_year xdesc 0! select sum_profit: sum rev - ps_supplycost * l_quantity by nat: n_name, o_year from ((((select l_orderkey, l_partkey, l_suppkey, l_quantity, rev: l_extendedprice * 1 - l_discount from lineitem where l_partkey in q9_pk) lj (`l_partkey`l_suppkey xkey select l_partkey: ps_partkey, l_suppkey: ps_suppkey, ps_supplycost from partsupp where ps_partkey in q9_pk)) lj (select o_year: first o_orderdate.year by l_orderkey: o_orderkey from orders)) lj ((select n_nationkey: first s_nationkey by l_suppkey: s_suppkey from supplier) lj nation))".into(),
        ),
        (
            "Q10 returned item reporting",
            "q10_ok: exec o_orderkey from orders where o_orderdate >= 1993.10.01, o_orderdate < 1994.01.01; \
             20 sublist `revenue xdesc select c_custkey, c_name, revenue, c_acctbal, n_name, c_address, c_phone, c_comment from ((0! select revenue: sum rev by c_custkey from (0! (select rev: sum l_extendedprice * 1 - l_discount by l_orderkey from lineitem where l_returnflag = \"R\", l_orderkey in q10_ok) ij (select c_custkey: first o_custkey by l_orderkey: o_orderkey from orders where o_orderkey in q10_ok))) lj (select first c_name, first c_acctbal, first c_address, first c_phone, first c_comment, n_nationkey: first c_nationkey by c_custkey from customer)) lj nation".into(),
        ),
        (
            "Q11 important stock",
            format!(
                "q11_sk: exec s_suppkey from supplier where s_nationkey in (exec n_nationkey from nation where n_name = `GERMANY); \
                 `stock_val xdesc select ps_partkey, stock_val from (0! select stock_val: sum ps_supplycost * ps_availqty by ps_partkey from partsupp where ps_suppkey in q11_sk) where stock_val > {frac} * sum stock_val"
            ),
        ),
        (
            "Q12 shipping modes",
            "q12_ok: distinct exec l_orderkey from lineitem where l_shipmode in `MAIL`SHIP, l_commitdate < l_receiptdate, l_shipdate < l_commitdate, l_receiptdate >= 1994.01.01, l_receiptdate < 1995.01.01; \
             q12_hi: exec o_orderkey from orders where o_orderkey in q12_ok, o_orderpriority in (`$\"1-URGENT\"; `$\"2-HIGH\"); \
             select high_line_count: sum l_orderkey in q12_hi, low_line_count: sum not l_orderkey in q12_hi by l_shipmode from lineitem where l_shipmode in `MAIL`SHIP, l_commitdate < l_receiptdate, l_shipdate < l_commitdate, l_receiptdate >= 1994.01.01, l_receiptdate < 1995.01.01".into(),
        ),
        (
            "Q13 customer distribution",
            "`custdist`c_count xdesc 0! select custdist: count i by c_count from (select c_custkey, c_count: 0 from customer) pj (select c_count: count i by c_custkey: o_custkey from orders where not o_comment like \"*special*requests*\")".into(),
        ),
        (
            "Q14 promotion effect",
            "q14_pp: exec p_partkey from part where p_type like \"PROMO*\"; \
             select promo_revenue: (100.0 * sum (l_extendedprice * 1 - l_discount) * l_partkey in q14_pp) % sum l_extendedprice * 1 - l_discount from lineitem where l_shipdate >= 1995.09.01, l_shipdate < 1995.10.01".into(),
        ),
        (
            "Q15 top supplier",
            "`s_suppkey xasc select s_suppkey, s_name, s_address, s_phone, total_revenue from (select s_suppkey, total_revenue from (0! select total_revenue: sum l_extendedprice * 1 - l_discount by s_suppkey: l_suppkey from lineitem where l_shipdate >= 1996.01.01, l_shipdate < 1996.04.01) where total_revenue = max total_revenue) lj supplier".into(),
        ),
        (
            "Q16 parts/supplier relationship",
            "q16_bad: exec s_suppkey from supplier where s_comment like \"*Customer*Complaints*\"; \
             q16_pk: exec p_partkey from part where p_brand <> `$\"Brand#45\", not p_type like \"MEDIUM POLISHED*\", p_size in 49 14 23 45 19 3 36 9; \
             `supplier_cnt xdesc `p_brand`p_type`p_size xasc 0! select supplier_cnt: count distinct ps_suppkey by p_brand, p_type, p_size from (select p_partkey: ps_partkey, ps_suppkey from partsupp where ps_partkey in q16_pk, not ps_suppkey in q16_bad) lj (select first p_brand, first p_type, first p_size by p_partkey from part where p_partkey in q16_pk)".into(),
        ),
        (
            "Q17 small-quantity-order revenue",
            "q17_pk: exec p_partkey from part where p_brand = `$\"Brand#23\", p_container = `$\"MED BOX\"; \
             select avg_yearly: (sum l_extendedprice) % 7.0 from (select l_partkey, l_quantity, l_extendedprice from lineitem where l_partkey in q17_pk) where l_quantity < 0.2 * (avg; l_quantity) fby l_partkey".into(),
        ),
        (
            "Q18 large volume customer",
            "q18_big: exec o_orderkey from (0! select sq: sum l_quantity by o_orderkey: l_orderkey from lineitem) where sq > 300; \
             100 sublist `o_totalprice xdesc `o_orderdate xasc select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum_qty from ((0! (select sum_qty: sum l_quantity by o_orderkey: l_orderkey from lineitem where l_orderkey in q18_big) ij (select c_custkey: first o_custkey, first o_orderdate, first o_totalprice by o_orderkey from orders where o_orderkey in q18_big)) lj (select first c_name by c_custkey from customer))".into(),
        ),
        (
            "Q19 discounted revenue",
            "q19_p1: exec p_partkey from part where p_brand = `$\"Brand#12\", p_container in (`$\"SM CASE\";`$\"SM BOX\";`$\"SM PACK\";`$\"SM PKG\"), p_size within 1 5; \
             q19_p2: exec p_partkey from part where p_brand = `$\"Brand#23\", p_container in (`$\"MED BAG\";`$\"MED BOX\";`$\"MED PKG\";`$\"MED PACK\"), p_size within 1 10; \
             q19_p3: exec p_partkey from part where p_brand = `$\"Brand#34\", p_container in (`$\"LG CASE\";`$\"LG BOX\";`$\"LG PACK\";`$\"LG PKG\"), p_size within 1 15; \
             select revenue: sum l_extendedprice * 1 - l_discount from lineitem where l_shipmode in (`AIR;`$\"AIR REG\"), l_shipinstruct = `$\"DELIVER IN PERSON\", ((l_partkey in q19_p1) & l_quantity within 1 11) | ((l_partkey in q19_p2) & l_quantity within 10 20) | ((l_partkey in q19_p3) & l_quantity within 20 30)".into(),
        ),
        (
            "Q20 potential part promotion",
            "q20_fp: exec p_partkey from part where p_name like \"forest*\"; \
             q20_cs: exec s_suppkey from supplier where s_nationkey in (exec n_nationkey from nation where n_name = `CANADA); \
             q20_sk: exec ps_suppkey from ((select ps_partkey, ps_suppkey, ps_availqty from partsupp where ps_partkey in q20_fp, ps_suppkey in q20_cs) ij (select half_q: 0.5 * sum l_quantity by ps_partkey: l_partkey, ps_suppkey: l_suppkey from lineitem where l_shipdate >= 1994.01.01, l_shipdate < 1995.01.01, l_partkey in q20_fp, l_suppkey in q20_cs)) where ps_availqty > half_q; \
             `s_name xasc select s_name, s_address from supplier where s_suppkey in q20_sk".into(),
        ),
        (
            "Q21 suppliers who kept orders waiting",
            "q21_sa: exec s_suppkey from supplier where s_nationkey in (exec n_nationkey from nation where n_name = `$\"SAUDI ARABIA\"); \
             q21_fok: exec o_orderkey from orders where o_orderstatus = \"F\", o_orderkey in (distinct exec l_orderkey from lineitem where l_suppkey in q21_sa, l_receiptdate > l_commitdate); \
             q21_m: exec l_orderkey from (0! select ns: count distinct l_suppkey by l_orderkey from lineitem where l_orderkey in q21_fok) where ns > 1; \
             q21_ok: exec l_orderkey from (0! select nls: count distinct l_suppkey by l_orderkey from lineitem where l_orderkey in q21_m, l_receiptdate > l_commitdate) where nls = 1; \
             100 sublist `numwait xdesc `s_name xasc 0! select numwait: count i by s_name from (select s_suppkey: l_suppkey from lineitem where l_orderkey in q21_ok, l_suppkey in q21_sa, l_receiptdate > l_commitdate) lj (select first s_name by s_suppkey from supplier where s_suppkey in q21_sa)".into(),
        ),
        (
            "Q22 global sales opportunity",
            "q22_avg: exec avg c_acctbal from (select c_acctbal, cc: `$2#'c_phone from customer where c_acctbal > 0) where cc in `13`31`23`29`30`18`17; \
             q22_ord: distinct exec o_custkey from orders; \
             select numcust: count i, totacctbal: sum c_acctbal by cntrycode from (select c_custkey, c_acctbal, cntrycode: `$2#'c_phone from customer where c_acctbal > q22_avg, not c_custkey in q22_ord) where cntrycode in `13`31`23`29`30`18`17".into(),
        ),
    ];
    v.into_iter().map(|(n, s)| (n.to_string(), s)).collect()
}

/// Recompute selected answers from the generated rows. Returns `None` for queries without a check, otherwise whether
/// the engine's rows match.
pub fn check(data: &Data, name: &str, rows: &[Vec<String>]) -> Option<Result<(), String>> {
    let money = |c: i64| c as f64 / 100.0;
    match name.split(' ').next().unwrap() {
        "Q1" => {
            let cut = d("1998-09-02");
            let mut g: std::collections::BTreeMap<(char, char), (i64, i64, f64, f64, i64, i64)> = Default::default();
            for l in data.lines.iter().filter(|l| l.ship <= cut) {
                let e = g.entry((l.rf, l.ls)).or_default();
                e.0 += l.qty;
                e.1 += l.price;
                let disc_price = money(l.price) * (1.0 - l.disc as f64 / 100.0);
                e.2 += disc_price;
                e.3 += 0.0;
                e.4 += l.disc;
                e.5 += 1;
            }
            if g.len() != rows.len() {
                return Some(Err(format!("{} groups, expected {}", rows.len(), g.len())));
            }
            for (((rf, ls), v), r) in g.iter().zip(rows) {
                let ok = r[0].trim() == rf.to_string()
                    && r[1].trim() == ls.to_string()
                    && close(num(&r[2]), v.0 as f64)
                    && close(num(&r[3]), money(v.1))
                    && close(num(&r[4]), v.2)
                    && num(&r[9]) as i64 == v.5
                    && close(num(&r[8]), v.4 as f64 / 100.0 / v.5 as f64);
                if !ok {
                    return Some(Err(format!("group {rf}{ls} differs: engine {r:?}")));
                }
            }
            Some(Ok(()))
        }
        "Q18" => {
            let mut sums: HashMap<i64, i64> = HashMap::new();
            for l in &data.lines {
                *sums.entry(l.orderkey).or_default() += l.qty;
            }
            let n = sums.values().filter(|v| **v > 300).count().min(100);
            if rows.len() == n {
                Some(Ok(()))
            } else {
                Some(Err(format!("{} rows, expected {}", rows.len(), n)))
            }
        }
        "Q6" => {
            let (lo, hi) = (d("1994-01-01"), d("1995-01-01"));
            let sum: i64 = data
                .lines
                .iter()
                .filter(|l| l.ship >= lo && l.ship < hi && (5..=7).contains(&l.disc) && l.qty < 24)
                .map(|l| l.price * l.disc)
                .sum();
            let got = num(&rows[0][0]);
            let want = sum as f64 / 10_000.0;
            if close(got, want) {
                Some(Ok(()))
            } else {
                Some(Err(format!("revenue {got} expected {want}")))
            }
        }
        "Q12" => {
            let (lo, hi) = (d("1994-01-01"), d("1995-01-01"));
            let mut g: std::collections::BTreeMap<&str, (i64, i64)> = Default::default();
            for l in data
                .lines
                .iter()
                .filter(|l| (l.mode == "MAIL" || l.mode == "SHIP") && l.commit < l.receipt && l.ship < l.commit && l.receipt >= lo && l.receipt < hi)
            {
                let high = matches!(data.orders[&l.orderkey].priority, "1-URGENT" | "2-HIGH");
                let e = g.entry(l.mode).or_default();
                if high { e.0 += 1 } else { e.1 += 1 }
            }
            if g.len() != rows.len() {
                return Some(Err(format!("{} modes, expected {}", rows.len(), g.len())));
            }
            for ((m, (h, lo_)), r) in g.iter().zip(rows) {
                if r[0].trim() != *m || num(&r[1]) as i64 != *h || num(&r[2]) as i64 != *lo_ {
                    return Some(Err(format!("{m} differs: engine {r:?}, expected {m} {h} {lo_}")));
                }
            }
            Some(Ok(()))
        }
        "Q13" => {
            let total: i64 = rows.iter().map(|r| num(&r[1]) as i64).sum();
            if total == data.customers as i64 {
                Some(Ok(()))
            } else {
                Some(Err(format!("distribution covers {total} customers, expected {}", data.customers)))
            }
        }
        "Q14" => {
            let (lo, hi) = (d("1995-09-01"), d("1995-10-01"));
            let (mut promo, mut all) = (0f64, 0f64);
            for l in data.lines.iter().filter(|l| l.ship >= lo && l.ship < hi) {
                let v = money(l.price) * (1.0 - l.disc as f64 / 100.0);
                all += v;
                if data.promo[&l.partkey] {
                    promo += v;
                }
            }
            let want = 100.0 * promo / all;
            let got = num(&rows[0][0]);
            if (got - want).abs() < 0.01 {
                Some(Ok(()))
            } else {
                Some(Err(format!("promo revenue {got} expected {want}")))
            }
        }
        _ => None,
    }
}
