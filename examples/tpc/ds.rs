//! TPC-DS: the core retail snowflake (three sales channels, returns, inventory, and the dimensions they share), a
//! columnar generator for it, and 24 queries in `q` / `qSQL` (star joins, `fby` correlated filters and partitioned
//! windows, multi-level rollups, channel `ij`/`lj` set operations, year-over-year comparisons, running `sums`).
//!
//! This is a subset: the full benchmark has 24 tables and 99 queries from templates that are not reproduced here, and
//! the generator is not `dsdgen`. Selected answers are recomputed in Rust from the generated rows.
#![allow(dead_code)]
use crate::util::*;
use chrono::{Datelike, Duration, NaiveDate};
use kixdb::types::NULL_LONG;
use kixdb::Connection;
use std::collections::HashMap;

pub const TABLES: &[&str] = &[
    "date_dim", "time_dim", "item", "customer_demographics", "household_demographics", "customer_address", "customer",
    "store", "promotion", "warehouse", "store_sales", "store_returns", "catalog_sales", "web_sales", "inventory",
];

pub const DISTRIBUTE: &[&str] = &[
    "DISTRIBUTE date_dim REPLICATED",
    "DISTRIBUTE time_dim REPLICATED",
    "DISTRIBUTE customer_demographics REPLICATED",
    "DISTRIBUTE household_demographics REPLICATED",
    "DISTRIBUTE customer_address REPLICATED",
    "DISTRIBUTE store REPLICATED",
    "DISTRIBUTE promotion REPLICATED",
    "DISTRIBUTE warehouse REPLICATED",
    "DISTRIBUTE item BY i_item_sk",
    "DISTRIBUTE customer BY c_customer_sk",
    "DISTRIBUTE store_sales BY ss_item_sk",
    "DISTRIBUTE store_returns BY sr_item_sk",
    "DISTRIBUTE catalog_sales BY cs_item_sk",
    "DISTRIBUTE web_sales BY ws_item_sk",
    "DISTRIBUTE inventory BY inv_item_sk",
];

pub const SCHEMA: &str = "
date_dim: ([d_date_sk: `long$()] d_date: `date$(); d_month_seq: `long$(); d_week_seq: `long$(); d_year: `long$(); d_dow: `long$(); d_moy: `long$(); d_dom: `long$(); d_qoy: `long$(); d_day_name: `$());
time_dim: ([t_time_sk: `long$()] t_time: `long$(); t_hour: `long$(); t_minute: `long$());
item: ([i_item_sk: `long$()] i_item_id: `$(); i_item_desc: (); i_current_price: `float$(); i_class: `$(); i_category_id: `long$(); i_category: `$(); i_brand_id: `long$(); i_brand: `$(); i_manufact_id: `long$(); i_manager_id: `long$(); i_color: `$());
customer_demographics: ([cd_demo_sk: `long$()] cd_gender: \"c\"$(); cd_marital_status: \"c\"$(); cd_education_status: `$(); cd_dep_count: `long$());
household_demographics: ([hd_demo_sk: `long$()] hd_income_band_sk: `long$(); hd_buy_potential: `$(); hd_dep_count: `long$(); hd_vehicle_count: `long$());
customer_address: ([ca_address_sk: `long$()] ca_state: `$(); ca_zip: `$(); ca_country: `$(); ca_city: `$());
customer: ([c_customer_sk: `long$()] c_customer_id: `$(); c_current_cdemo_sk: `long$(); c_current_hdemo_sk: `long$(); c_current_addr_sk: `long$(); c_first_name: `$(); c_last_name: `$(); c_birth_year: `long$());
store: ([s_store_sk: `long$()] s_store_id: `$(); s_store_name: `$(); s_state: `$(); s_zip: `$(); s_number_employees: `long$());
promotion: ([p_promo_sk: `long$()] p_channel_email: \"c\"$(); p_channel_event: \"c\"$(); p_channel_tv: \"c\"$());
warehouse: ([w_warehouse_sk: `long$()] w_warehouse_name: `$(); w_state: `$());
store_sales: ([] ss_sold_date_sk: `long$(); ss_sold_time_sk: `long$(); ss_item_sk: `long$(); ss_customer_sk: `long$(); ss_cdemo_sk: `long$(); ss_hdemo_sk: `long$(); ss_addr_sk: `long$(); ss_store_sk: `long$(); ss_promo_sk: `long$(); ss_ticket_number: `long$(); ss_quantity: `long$(); ss_list_price: `float$(); ss_sales_price: `float$(); ss_ext_sales_price: `float$(); ss_coupon_amt: `float$(); ss_net_profit: `float$());
store_returns: ([] sr_returned_date_sk: `long$(); sr_item_sk: `long$(); sr_customer_sk: `long$(); sr_store_sk: `long$(); sr_ticket_number: `long$(); sr_return_quantity: `long$(); sr_return_amt: `float$());
catalog_sales: ([] cs_sold_date_sk: `long$(); cs_item_sk: `long$(); cs_bill_customer_sk: `long$(); cs_bill_cdemo_sk: `long$(); cs_promo_sk: `long$(); cs_order_number: `long$(); cs_quantity: `long$(); cs_list_price: `float$(); cs_sales_price: `float$(); cs_ext_sales_price: `float$(); cs_coupon_amt: `float$(); cs_net_profit: `float$());
web_sales: ([] ws_sold_date_sk: `long$(); ws_item_sk: `long$(); ws_bill_customer_sk: `long$(); ws_order_number: `long$(); ws_quantity: `long$(); ws_sales_price: `float$(); ws_ext_sales_price: `float$(); ws_net_profit: `float$());
inventory: ([] inv_date_sk: `long$(); inv_item_sk: `long$(); inv_warehouse_sk: `long$(); inv_quantity_on_hand: `long$())
";

const CATEGORIES: [&str; 10] = ["Women", "Men", "Children", "Shoes", "Music", "Jewelry", "Home", "Sports", "Books", "Electronics"];
const STATES: [&str; 12] = ["TN", "SD", "AL", "GA", "KY", "NM", "CA", "WA", "TX", "OH", "VA", "NY"];
const EDUCATION: [&str; 7] = ["Primary", "Secondary", "College", "2 yr Degree", "4 yr Degree", "Advanced Degree", "Unknown"];
const COLORS: [&str; 8] = ["red", "blue", "green", "white", "black", "yellow", "brown", "pink"];
const DAYS: [&str; 7] = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const FIRST: [&str; 10] = ["James", "Mary", "John", "Linda", "Robert", "Susan", "Michael", "Karen", "David", "Nancy"];
const LAST: [&str; 10] = ["Smith", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Clark", "Hall"];

const BASE: NaiveDate = match NaiveDate::from_ymd_opt(1998, 1, 1) {
    Some(d) => d,
    None => panic!(),
};
const DATE_SK0: i64 = 2_450_815;
const DAYS_SPAN: i64 = 1826; // 1998-01-01 .. 2002-12-31

struct Sale {
    date: i64,
    time: i64,
    item: i64,
    hdemo: i64,
    store: i64,
    qty: i64,
    ext: i64, // cents
}

struct ItemInfo {
    manufact: i64,
    manager: i64,
    brand_id: i64,
    brand: String,
    cat_id: i64,
    cat: String,
}

pub struct Data {
    sales: Vec<Sale>,
    items: HashMap<i64, ItemInfo>,
    dates: HashMap<i64, (i32, u32)>, // sk -> (year, month)
    hd_dep: HashMap<i64, i64>,
    store_names: HashMap<i64, String>,
}

fn cents(c: i64) -> f64 {
    c as f64 / 100.0
}

fn opt_long(v: Option<i64>) -> i64 {
    v.unwrap_or(NULL_LONG)
}

pub fn load(c: &mut Connection, sf: f64) -> Data {
    must(c, SCHEMA);
    let mut rng = Rng::new(20_000_101);
    let scale = (sf / 0.01).max(0.1);
    let n_item = ((300.0 * scale) as i64).max(300);
    let n_cust = ((1000.0 * scale) as i64).max(300);
    let n_addr = ((500.0 * scale) as i64).max(300);
    let n_store = 12i64;

    let mut dates_map = HashMap::with_capacity(DAYS_SPAN as usize);
    let (mut d_sk, mut d_dt, mut d_ms, mut d_ws, mut d_yr, mut d_dw, mut d_my, mut d_dm, mut d_qy, mut d_dn) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    for off in 0..DAYS_SPAN {
        let dt = BASE + Duration::days(off);
        let sk = DATE_SK0 + off;
        let (y, m) = (dt.year(), dt.month());
        let dow = dt.weekday().num_days_from_sunday() as i64;
        dates_map.insert(sk, (y, m));
        d_sk.push(sk);
        d_dt.push(k_date(dt));
        d_ms.push(((y - 1900) * 12 + m as i32 - 1) as i64);
        d_ws.push(off / 7 + 1);
        d_yr.push(y as i64);
        d_dw.push(dow);
        d_my.push(m as i64);
        d_dm.push(dt.day() as i64);
        d_qy.push(((m + 2) / 3) as i64);
        d_dn.push(DAYS[dow as usize]);
    }
    load_cols(c, "date_dim", vec![
        ("d_date_sk", longs(d_sk)), ("d_date", dates(d_dt)), ("d_month_seq", longs(d_ms)), ("d_week_seq", longs(d_ws)),
        ("d_year", longs(d_yr)), ("d_dow", longs(d_dw)), ("d_moy", longs(d_my)), ("d_dom", longs(d_dm)),
        ("d_qoy", longs(d_qy)), ("d_day_name", syms(d_dn)),
    ]);
    load_cols(c, "time_dim", vec![
        ("t_time_sk", longs((0..1440).collect())), ("t_time", longs((0..1440).map(|m| m * 60).collect())),
        ("t_hour", longs((0..1440).map(|m| m / 60).collect())), ("t_minute", longs((0..1440).map(|m| m % 60).collect())),
    ]);

    let mut items = HashMap::with_capacity(n_item as usize);
    let (mut i_sk, mut i_id, mut i_ds, mut i_cp, mut i_cl, mut i_ci, mut i_ca, mut i_bi, mut i_br, mut i_mf, mut i_mg, mut i_co) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    for i in 1..=n_item {
        let cat_id = rng.int(1, 10);
        let cat = CATEGORIES[(cat_id - 1) as usize];
        let manufact = if i % 150 == 1 { 128 } else { rng.int(1, 200) };
        let manager = rng.int(1, 100);
        let brand_id = cat_id * 1_000_000 + rng.int(1, 20) * 1000 + rng.int(1, 9);
        let brand = format!("brand #{}", brand_id % 997);
        let price = rng.int(100, 20_000);
        let desc = format!("{cat} item {}", rng.text(3));
        let cls = format!("{cat} class {}", rng.int(1, 5));
        let col = *rng.pick(&COLORS);
        i_sk.push(i);
        i_id.push(format!("AAAAAAAA{i:08}"));
        i_ds.push(desc);
        i_cp.push(cents(price));
        i_cl.push(cls);
        i_ci.push(cat_id);
        i_ca.push(cat);
        i_bi.push(brand_id);
        i_br.push(brand.clone());
        i_mf.push(manufact);
        i_mg.push(manager);
        i_co.push(col);
        items.insert(i, ItemInfo { manufact, manager, brand_id, brand, cat_id, cat: cat.to_string() });
    }
    load_cols(c, "item", vec![
        ("i_item_sk", longs(i_sk)), ("i_item_id", syms(i_id)), ("i_item_desc", strings(i_ds)), ("i_current_price", floats(i_cp)),
        ("i_class", syms(i_cl)), ("i_category_id", longs(i_ci)), ("i_category", syms(i_ca)), ("i_brand_id", longs(i_bi)),
        ("i_brand", syms(i_br)), ("i_manufact_id", longs(i_mf)), ("i_manager_id", longs(i_mg)), ("i_color", syms(i_co)),
    ]);

    let (mut cd_sk, mut cd_g, mut cd_m, mut cd_e, mut cd_d) = (vec![], vec![], vec![], vec![], vec![]);
    let mut sk = 0i64;
    for g in [b'M', b'F'] {
        for m in [b'M', b'S', b'D', b'W', b'U'] {
            for e in EDUCATION {
                for dep in 0..4 {
                    sk += 1;
                    cd_sk.push(sk);
                    cd_g.push(g);
                    cd_m.push(m);
                    cd_e.push(e);
                    cd_d.push(dep);
                }
            }
        }
    }
    let n_cd = sk;
    load_cols(c, "customer_demographics", vec![
        ("cd_demo_sk", longs(cd_sk)), ("cd_gender", chars(cd_g)), ("cd_marital_status", chars(cd_m)),
        ("cd_education_status", syms(cd_e)), ("cd_dep_count", longs(cd_d)),
    ]);

    let mut hd_dep = HashMap::new();
    let n_hd = 600i64;
    let (mut hd_sk, mut hd_ib, mut hd_bp, mut hd_dc, mut hd_vc) = (vec![], vec![], vec![], vec![], vec![]);
    for h in 1..=n_hd {
        let dep = rng.int(0, 9);
        hd_dep.insert(h, dep);
        hd_sk.push(h);
        hd_ib.push(rng.int(1, 20));
        hd_bp.push(*rng.pick(&[">10000", "5001-10000", "1001-5000", "501-1000", "0-500", "Unknown"]));
        hd_dc.push(dep);
        hd_vc.push(rng.int(0, 4));
    }
    load_cols(c, "household_demographics", vec![
        ("hd_demo_sk", longs(hd_sk)), ("hd_income_band_sk", longs(hd_ib)), ("hd_buy_potential", syms(hd_bp)),
        ("hd_dep_count", longs(hd_dc)), ("hd_vehicle_count", longs(hd_vc)),
    ]);

    let (mut ca_sk, mut ca_st, mut ca_zp, mut ca_ct) = (vec![], vec![], vec![], vec![]);
    for a in 1..=n_addr {
        ca_sk.push(a);
        ca_st.push(*rng.pick(&STATES));
        ca_zp.push(if a % 9 == 0 { "85669".to_string() } else { rng.digits(5) });
        ca_ct.push(format!("City {}", rng.int(1, 60)));
    }
    load_cols(c, "customer_address", vec![
        ("ca_address_sk", longs(ca_sk)), ("ca_state", syms(ca_st)), ("ca_zip", syms(ca_zp)),
        ("ca_country", syms(vec!["United States"; n_addr as usize])), ("ca_city", syms(ca_ct)),
    ]);

    let (mut c_sk, mut c_id, mut c_cd, mut c_hd, mut c_ad, mut c_fn, mut c_ln, mut c_by) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    for k in 1..=n_cust {
        c_sk.push(k);
        c_id.push(format!("AAAAAAAA{k:08}"));
        c_cd.push(rng.int(1, n_cd));
        c_hd.push(rng.int(1, n_hd));
        c_ad.push(rng.int(1, n_addr));
        c_fn.push(*rng.pick(&FIRST));
        c_ln.push(*rng.pick(&LAST));
        c_by.push(rng.int(1930, 1992));
    }
    load_cols(c, "customer", vec![
        ("c_customer_sk", longs(c_sk)), ("c_customer_id", syms(c_id)), ("c_current_cdemo_sk", longs(c_cd)),
        ("c_current_hdemo_sk", longs(c_hd)), ("c_current_addr_sk", longs(c_ad)), ("c_first_name", syms(c_fn)),
        ("c_last_name", syms(c_ln)), ("c_birth_year", longs(c_by)),
    ]);

    let store_names = ["ese", "ought", "able", "bar", "eing", "ation", "anti", "cally", "n st", "pri", "ese2", "ought2"];
    let (mut s_sk, mut s_id, mut s_nm, mut s_st, mut s_zp, mut s_em) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for s in 1..=n_store {
        s_sk.push(s);
        s_id.push(format!("AAAAAAAA{s:08}"));
        s_nm.push(store_names[(s - 1) as usize]);
        s_st.push(*rng.pick(&STATES));
        s_zp.push(rng.digits(5));
        s_em.push(rng.int(200, 300));
    }
    load_cols(c, "store", vec![
        ("s_store_sk", longs(s_sk)), ("s_store_id", syms(s_id)), ("s_store_name", syms(s_nm)),
        ("s_state", syms(s_st)), ("s_zip", syms(s_zp)), ("s_number_employees", longs(s_em)),
    ]);

    let (mut p_sk, mut p_em, mut p_ev, mut p_tv) = (vec![], vec![], vec![], vec![]);
    for p in 1..=30 {
        p_sk.push(p);
        p_em.push(*rng.pick(&[b'Y', b'N']));
        p_ev.push(*rng.pick(&[b'Y', b'N']));
        p_tv.push(*rng.pick(&[b'Y', b'N']));
    }
    load_cols(c, "promotion", vec![
        ("p_promo_sk", longs(p_sk)), ("p_channel_email", chars(p_em)), ("p_channel_event", chars(p_ev)), ("p_channel_tv", chars(p_tv)),
    ]);

    let (mut w_sk, mut w_nm, mut w_st) = (vec![], vec![], vec![]);
    for w in 1..=5 {
        w_sk.push(w);
        w_nm.push(format!("Warehouse {w}"));
        w_st.push(*rng.pick(&STATES));
    }
    load_cols(c, "warehouse", vec![("w_warehouse_sk", longs(w_sk)), ("w_warehouse_name", syms(w_nm)), ("w_state", syms(w_st))]);

    let n_tickets = ((4000.0 * scale) as i64).max(2000);
    let mut sales = Vec::new();
    let (mut ss_dt, mut ss_tm, mut ss_it, mut ss_cu, mut ss_cd, mut ss_hd, mut ss_ad, mut ss_st, mut ss_pr, mut ss_tk, mut ss_qt, mut ss_lp, mut ss_sp, mut ss_ex, mut ss_cp, mut ss_np) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    let (mut sr_dt, mut sr_it, mut sr_cu, mut sr_st, mut sr_tk, mut sr_qt, mut sr_am) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);

    for t in 1..=n_tickets {
        let date = DATE_SK0 + rng.int(0, DAYS_SPAN - 1);
        let time = rng.int(8 * 60, 21 * 60 + 59);
        let cust = if rng.chance(0.02) { None } else { Some(rng.int(1, n_cust)) };
        let (cd, hd, addr, store, promo) = (rng.int(1, n_cd), rng.int(1, n_hd), rng.int(1, n_addr), rng.int(1, n_store), rng.int(1, 30));
        let mut used = vec![];
        for _ in 0..rng.int(1, 8) {
            let item = rng.int(1, n_item);
            if used.contains(&item) {
                continue;
            }
            used.push(item);
            let qty = rng.int(1, 100);
            let list = rng.int(100, 20_000);
            let price = list * rng.int(50, 100) / 100;
            let ext = price * qty;
            let coup = rng.int(0, ext / 10);
            let prof = rng.int(-ext / 4, ext / 3);

            ss_dt.push(date); ss_tm.push(time); ss_it.push(item); ss_cu.push(opt_long(cust));
            ss_cd.push(cd); ss_hd.push(hd); ss_ad.push(addr); ss_st.push(store); ss_pr.push(promo);
            ss_tk.push(t); ss_qt.push(qty); ss_lp.push(cents(list)); ss_sp.push(cents(price));
            ss_ex.push(cents(ext)); ss_cp.push(cents(coup)); ss_np.push(cents(prof));
            sales.push(Sale { date, time, item, hdemo: hd, store, qty, ext });

            if rng.chance(0.1) {
                let rdate = (date + rng.int(1, 90)).min(DATE_SK0 + DAYS_SPAN - 1);
                let rq = rng.int(1, qty);
                sr_dt.push(rdate); sr_it.push(item); sr_cu.push(opt_long(cust));
                sr_st.push(store); sr_tk.push(t); sr_qt.push(rq); sr_am.push(cents(price * rq));
            }
        }
    }
    load_cols(c, "store_sales", vec![
        ("ss_sold_date_sk", longs(ss_dt)), ("ss_sold_time_sk", longs(ss_tm)), ("ss_item_sk", longs(ss_it)),
        ("ss_customer_sk", longs(ss_cu)), ("ss_cdemo_sk", longs(ss_cd)), ("ss_hdemo_sk", longs(ss_hd)),
        ("ss_addr_sk", longs(ss_ad)), ("ss_store_sk", longs(ss_st)), ("ss_promo_sk", longs(ss_pr)),
        ("ss_ticket_number", longs(ss_tk)), ("ss_quantity", longs(ss_qt)), ("ss_list_price", floats(ss_lp)),
        ("ss_sales_price", floats(ss_sp)), ("ss_ext_sales_price", floats(ss_ex)), ("ss_coupon_amt", floats(ss_cp)),
        ("ss_net_profit", floats(ss_np)),
    ]);
    load_cols(c, "store_returns", vec![
        ("sr_returned_date_sk", longs(sr_dt)), ("sr_item_sk", longs(sr_it)), ("sr_customer_sk", longs(sr_cu)),
        ("sr_store_sk", longs(sr_st)), ("sr_ticket_number", longs(sr_tk)), ("sr_return_quantity", longs(sr_qt)),
        ("sr_return_amt", floats(sr_am)),
    ]);

    let (mut cs_dt, mut cs_it, mut cs_cu, mut cs_cd, mut cs_pr, mut cs_on, mut cs_qt, mut cs_lp, mut cs_sp, mut cs_ex, mut cs_cp, mut cs_np) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    let (mut ws_dt, mut ws_it, mut ws_cu, mut ws_on, mut ws_qt, mut ws_sp, mut ws_ex, mut ws_np) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);

    for o in 1..=(n_tickets / 2) {
        let date = DATE_SK0 + rng.int(0, DAYS_SPAN - 1);
        let cust = rng.int(1, n_cust);
        let mut used = vec![];
        for _ in 0..rng.int(1, 5) {
            let item = rng.int(1, n_item);
            if used.contains(&item) {
                continue;
            }
            used.push(item);
            let qty = rng.int(1, 100);
            let list = rng.int(100, 20_000);
            let price = list * rng.int(50, 100) / 100;
            let ext = price * qty;
            let cd = rng.int(1, n_cd);
            let pr = rng.int(1, 30);
            let coup = rng.int(0, ext / 10);
            let prof = rng.int(-ext / 4, ext / 3);
            cs_dt.push(date); cs_it.push(item); cs_cu.push(cust); cs_cd.push(cd); cs_pr.push(pr); cs_on.push(o);
            cs_qt.push(qty); cs_lp.push(cents(list)); cs_sp.push(cents(price)); cs_ex.push(cents(ext));
            cs_cp.push(cents(coup)); cs_np.push(cents(prof));
        }
        let mut used = vec![];
        for _ in 0..rng.int(0, 4) {
            let item = rng.int(1, n_item);
            if used.contains(&item) {
                continue;
            }
            used.push(item);
            let qty = rng.int(1, 100);
            let price = rng.int(100, 15_000);
            let wdt = DATE_SK0 + rng.int(0, DAYS_SPAN - 1);
            let wcu = rng.int(1, n_cust);
            let prof = rng.int(-price * qty / 4, price * qty / 3);
            ws_dt.push(wdt); ws_it.push(item); ws_cu.push(wcu); ws_on.push(o);
            ws_qt.push(qty); ws_sp.push(cents(price)); ws_ex.push(cents(price * qty)); ws_np.push(cents(prof));
        }
    }
    load_cols(c, "catalog_sales", vec![
        ("cs_sold_date_sk", longs(cs_dt)), ("cs_item_sk", longs(cs_it)), ("cs_bill_customer_sk", longs(cs_cu)),
        ("cs_bill_cdemo_sk", longs(cs_cd)), ("cs_promo_sk", longs(cs_pr)), ("cs_order_number", longs(cs_on)),
        ("cs_quantity", longs(cs_qt)), ("cs_list_price", floats(cs_lp)), ("cs_sales_price", floats(cs_sp)),
        ("cs_ext_sales_price", floats(cs_ex)), ("cs_coupon_amt", floats(cs_cp)), ("cs_net_profit", floats(cs_np)),
    ]);
    load_cols(c, "web_sales", vec![
        ("ws_sold_date_sk", longs(ws_dt)), ("ws_item_sk", longs(ws_it)), ("ws_bill_customer_sk", longs(ws_cu)),
        ("ws_order_number", longs(ws_on)), ("ws_quantity", longs(ws_qt)), ("ws_sales_price", floats(ws_sp)),
        ("ws_ext_sales_price", floats(ws_ex)), ("ws_net_profit", floats(ws_np)),
    ]);

    let cap_inv = 8 * 5 * n_item as usize;
    let (mut iv_dt, mut iv_it, mut iv_wh, mut iv_qh) =
        (Vec::with_capacity(cap_inv), Vec::with_capacity(cap_inv), Vec::with_capacity(cap_inv), Vec::with_capacity(cap_inv));
    for k in 0..8 {
        let date = DATE_SK0 + 700 + k * 28;
        for i in 1..=n_item {
            for w in 1..=5 {
                iv_dt.push(date);
                iv_it.push(i);
                iv_wh.push(w);
                iv_qh.push(rng.int(0, 1000));
            }
        }
    }
    load_cols(c, "inventory", vec![
        ("inv_date_sk", longs(iv_dt)), ("inv_item_sk", longs(iv_it)), ("inv_warehouse_sk", longs(iv_wh)), ("inv_quantity_on_hand", longs(iv_qh)),
    ]);

    let store_names = store_names.iter().enumerate().map(|(i, n)| (i as i64 + 1, n.to_string())).collect();
    Data { sales, items, dates: dates_map, hd_dep, store_names }
}

pub fn queries() -> Vec<(String, String)> {
    let v: Vec<(&str, &str)> = vec![
        (
            "Q1 customers returning more than average",
            "q1_d: exec d_date_sk from date_dim where d_year = 2000; \
             q1_tn: exec s_store_sk from store where s_state = `TN; \
             100 sublist `c_customer_id xasc select c_customer_id from ((select c_customer_sk, s_store_sk, ctr: ctr_total_return from (0! select ctr_total_return: sum sr_return_amt by c_customer_sk: sr_customer_sk, s_store_sk: sr_store_sk from store_returns where sr_returned_date_sk in q1_d) where s_store_sk in q1_tn, ctr_total_return > 1.2 * (avg; ctr_total_return) fby s_store_sk) ij (select first c_customer_id by c_customer_sk from customer))",
        ),
        (
            "Q3 brand sales for a manufacturer",
            "q3_i: exec i_item_sk from item where i_manufact_id = 128; \
             q3_d: exec d_date_sk from date_dim where d_moy = 11; \
             100 sublist `d_year xasc `sum_agg xdesc `brand_id xasc 0! select sum_agg: sum s_ext by d_year, brand_id: i_brand_id, brand: i_brand from ((0! select s_ext: sum ss_ext_sales_price by d_date_sk: ss_sold_date_sk, i_item_sk: ss_item_sk from store_sales where ss_item_sk in q3_i, ss_sold_date_sk in q3_d) lj (select first i_brand_id, first i_brand by i_item_sk from item where i_item_sk in q3_i)) lj date_dim",
        ),
        (
            "Q6 states with expensive items",
            "q6_d: exec d_date_sk from date_dim where d_month_seq in (distinct exec d_month_seq from date_dim where d_year = 2001, d_moy = 1); \
             q6_i: exec i_item_sk from (select i_item_sk, i_current_price, i_category from item) where i_current_price > 1.2 * (avg; i_current_price) fby i_category; \
             100 sublist `cnt`state xasc select state, cnt from (0! select cnt: sum n by state: ca_state from ((0! select n: count i by c_customer_sk: ss_customer_sk from store_sales where ss_sold_date_sk in q6_d, ss_item_sk in q6_i, not null ss_customer_sk) ij (select ca_address_sk: first c_current_addr_sk by c_customer_sk from customer)) lj customer_address) where cnt >= 1",
        ),
        (
            "Q7 promotion averages",
            "q7_d: exec d_date_sk from date_dim where d_year = 2000; \
             q7_cd: exec cd_demo_sk from customer_demographics where cd_gender = \"M\", cd_marital_status = \"S\", cd_education_status = `College; \
             q7_p: exec p_promo_sk from promotion where (p_channel_email = \"N\") | (p_channel_event = \"N\"); \
             100 sublist select agg1: avg ss_quantity, agg2: avg ss_list_price, agg3: avg ss_coupon_amt, agg4: avg ss_sales_price by i_item_id from (select i_item_sk: ss_item_sk, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price from store_sales where ss_sold_date_sk in q7_d, ss_cdemo_sk in q7_cd, ss_promo_sk in q7_p) lj (select first i_item_id by i_item_sk from item)",
        ),
        (
            "Q12 web revenue ratio per class",
            "q12_d: exec d_date_sk from date_dim where d_date within 1999.02.22 1999.03.24; \
             q12_i: exec i_item_sk from item where i_category in `Sports`Books`Home; \
             100 sublist `i_category`i_class`i_item_id`i_item_desc`revenueratio xasc update revenueratio: 100.0 * itemrevenue % (sum; itemrevenue) fby i_class from 0! select itemrevenue: sum rev by i_item_id, i_item_desc, i_category, i_class, i_current_price from (0! select rev: sum ws_ext_sales_price by i_item_sk: ws_item_sk from web_sales where ws_sold_date_sk in q12_d, ws_item_sk in q12_i) lj (select first i_item_id, first i_item_desc, first i_category, first i_class, first i_current_price by i_item_sk from item where i_item_sk in q12_i)",
        ),
        (
            "Q15 catalog sales by zip",
            "q15_d: exec d_date_sk from date_dim where d_qoy = 2, d_year = 2001; \
             q15_a: exec ca_address_sk from customer_address where (ca_zip in `85669`86197`88274`83405`86475`85392`85460`80348`81792) | ca_state in `CA`WA`GA; \
             q15_c: exec c_customer_sk from customer where c_current_addr_sk in q15_a; \
             100 sublist select sum_sales: sum s_price by ca_zip from ((0! select s_price: sum cs_sales_price by c_customer_sk: cs_bill_customer_sk from catalog_sales where cs_sold_date_sk in q15_d, (cs_bill_customer_sk in q15_c) | cs_sales_price > 500) ij (select ca_address_sk: first c_current_addr_sk by c_customer_sk from customer)) lj customer_address",
        ),
        (
            "Q19 brand sales outside the zip",
            "q19_d: exec d_date_sk from date_dim where d_moy = 11, d_year = 1998; \
             q19_i: exec i_item_sk from item where i_manager_id = 8; \
             100 sublist `ext_price xdesc `brand`brand_id`i_manufact_id xasc `brand_id`brand`i_manufact_id`ext_price xcols 0! select ext_price: sum ss_ext_sales_price by brand_id: i_brand_id, brand: i_brand, i_manufact_id from ((((select i_item_sk: ss_item_sk, c_customer_sk: ss_customer_sk, s_store_sk: ss_store_sk, ss_ext_sales_price from store_sales where ss_sold_date_sk in q19_d, ss_item_sk in q19_i, not null ss_customer_sk) ij (select ca_address_sk: first c_current_addr_sk by c_customer_sk from customer)) lj customer_address) lj store) lj (select first i_brand_id, first i_brand, first i_manufact_id by i_item_sk from item where i_item_sk in q19_i) where ca_zip <> s_zip",
        ),
        (
            "Q20 catalog revenue ratio per class",
            "q20_d: exec d_date_sk from date_dim where d_date within 1999.02.22 1999.03.24; \
             q20_i: exec i_item_sk from item where i_category in `Sports`Books`Home; \
             100 sublist `i_category`i_class`i_item_id`revenueratio xasc update revenueratio: 100.0 * itemrevenue % (sum; itemrevenue) fby i_class from 0! select itemrevenue: sum rev by i_item_id, i_category, i_class, i_current_price from (0! select rev: sum cs_ext_sales_price by i_item_sk: cs_item_sk from catalog_sales where cs_sold_date_sk in q20_d, cs_item_sk in q20_i) lj (select first i_item_id, first i_category, first i_class, first i_current_price by i_item_sk from item where i_item_sk in q20_i)",
        ),
        (
            "Q22 inventory rollup",
            "q22_d: exec d_date_sk from date_dim where d_month_seq within 1200 1211; \
             q22_b: (0! select s: sum inv_quantity_on_hand, n: count i by i_item_sk: inv_item_sk from inventory where inv_date_sk in q22_d) lj (select first i_item_id, first i_category by i_item_sk from item); \
             q22_0: 0! select qoh: (sum s) % sum n by i_item_id, i_category from q22_b; \
             q22_1: select i_item_id: `, i_category, qoh from 0! select qoh: (sum s) % sum n by i_category from q22_b; \
             q22_2: $[count q22_b; select i_item_id: `, i_category: `, qoh: (sum s) % sum n from q22_b; 0#q22_0]; \
             100 sublist `qoh`i_category`i_item_id xasc q22_0, q22_1, q22_2",
        ),
        (
            "Q26 catalog promotion averages",
            "q26_d: exec d_date_sk from date_dim where d_year = 2000; \
             q26_cd: exec cd_demo_sk from customer_demographics where cd_gender = \"M\", cd_marital_status = \"S\", cd_education_status = `College; \
             q26_p: exec p_promo_sk from promotion where (p_channel_email = \"N\") | (p_channel_event = \"N\"); \
             100 sublist select agg1: avg cs_quantity, agg2: avg cs_list_price, agg3: avg cs_coupon_amt, agg4: avg cs_sales_price by i_item_id from (select i_item_sk: cs_item_sk, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price from catalog_sales where cs_sold_date_sk in q26_d, cs_bill_cdemo_sk in q26_cd, cs_promo_sk in q26_p) lj (select first i_item_id by i_item_sk from item)",
        ),
        (
            "Q27 state rollup with grouping",
            "q27_d: exec d_date_sk from date_dim where d_year = 2002; \
             q27_cd: exec cd_demo_sk from customer_demographics where cd_gender = \"M\", cd_marital_status = \"S\", cd_education_status = `College; \
             q27_s: exec s_store_sk from store where s_state in `TN`SD`AL`GA; \
             q27_b: ((select i_item_sk: ss_item_sk, s_store_sk: ss_store_sk, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price from store_sales where ss_sold_date_sk in q27_d, ss_cdemo_sk in q27_cd, ss_store_sk in q27_s) lj (select first i_item_id by i_item_sk from item)) lj (select first s_state by s_store_sk from store); \
             q27_0: select i_item_id, s_state, g_state: 0, agg1, agg2, agg3, agg4 from 0! select agg1: avg ss_quantity, agg2: avg ss_list_price, agg3: avg ss_coupon_amt, agg4: avg ss_sales_price by i_item_id, s_state from q27_b; \
             q27_1: select i_item_id, s_state: `, g_state: 1, agg1, agg2, agg3, agg4 from 0! select agg1: avg ss_quantity, agg2: avg ss_list_price, agg3: avg ss_coupon_amt, agg4: avg ss_sales_price by i_item_id from q27_b; \
             q27_2: $[count q27_b; select i_item_id: `, s_state: `, g_state: 1, agg1: avg ss_quantity, agg2: avg ss_list_price, agg3: avg ss_coupon_amt, agg4: avg ss_sales_price from q27_b; 0#q27_0]; \
             100 sublist `i_item_id`s_state xasc q27_0, q27_1, q27_2",
        ),
        (
            "Q36 gross margin rank within parent",
            "q36_d: exec d_date_sk from date_dim where d_year = 2001; \
             q36_s: exec s_store_sk from store where s_state in `TN`SD`AL`GA; \
             q36_b: (0! select np: sum ss_net_profit, ext: sum ss_ext_sales_price by i_item_sk: ss_item_sk from store_sales where ss_sold_date_sk in q36_d, ss_store_sk in q36_s) lj (select first i_category, first i_class by i_item_sk from item); \
             q36_0: `i_category`rank_within_parent xasc update rank_within_parent: ({1 + (asc x) ? x}; gross_margin) fby i_category from select gross_margin, i_category, i_class, lochierarchy: 0 from 0! select gross_margin: (sum np) % sum ext by i_category, i_class from q36_b; \
             q36_1: `rank_within_parent xasc update rank_within_parent: 1 + (asc gross_margin) ? gross_margin from select gross_margin, i_category, i_class: `, lochierarchy: 1 from 0! select gross_margin: (sum np) % sum ext by i_category from q36_b; \
             q36_2: $[count q36_b; select gross_margin: (sum np) % sum ext, i_category: `, i_class: `, lochierarchy: 2, rank_within_parent: 1 from q36_b; 0#q36_0]; \
             100 sublist q36_2, q36_1, q36_0",
        ),
        (
            "Q38 customers buying on all channels",
            "q38_d: exec d_date_sk from date_dim where d_month_seq within 1200 1211; \
             select cnt: count i from (((select ok: 1b by c_last_name, c_first_name, d_date from ((select distinct c_customer_sk: ss_customer_sk, d_date_sk: ss_sold_date_sk from store_sales where ss_sold_date_sk in q38_d, not null ss_customer_sk) ij (select first c_last_name, first c_first_name by c_customer_sk from customer)) lj (select first d_date by d_date_sk from date_dim)) ij (select ok: 1b by c_last_name, c_first_name, d_date from ((select distinct c_customer_sk: cs_bill_customer_sk, d_date_sk: cs_sold_date_sk from catalog_sales where cs_sold_date_sk in q38_d) ij (select first c_last_name, first c_first_name by c_customer_sk from customer)) lj (select first d_date by d_date_sk from date_dim))) ij (select ok: 1b by c_last_name, c_first_name, d_date from ((select distinct c_customer_sk: ws_bill_customer_sk, d_date_sk: ws_sold_date_sk from web_sales where ws_sold_date_sk in q38_d) ij (select first c_last_name, first c_first_name by c_customer_sk from customer)) lj (select first d_date by d_date_sk from date_dim)))",
        ),
        (
            "Q42 category sales for a manager",
            "q42_d: exec d_date_sk from date_dim where d_moy = 11, d_year = 2000; \
             q42_i: exec i_item_sk from item where i_manager_id = 1; \
             100 sublist `sum_ext xdesc `d_year`i_category_id`i_category xasc 0! select sum_ext: sum s_ext by d_year: 2000, i_category_id, i_category from (0! select s_ext: sum ss_ext_sales_price by i_item_sk: ss_item_sk from store_sales where ss_sold_date_sk in q42_d, ss_item_sk in q42_i) lj (select first i_category_id, first i_category by i_item_sk from item where i_item_sk in q42_i)",
        ),
        (
            "Q43 store sales by weekday",
            "q43_d: exec d_date_sk from date_dim where d_year = 2000; \
             100 sublist `s_store_name`s_store_id`sun_sales`mon_sales xasc 0! select sun_sales: $[sum n * d_day_name = `Sunday; sum sp * d_day_name = `Sunday; 0n], mon_sales: $[sum n * d_day_name = `Monday; sum sp * d_day_name = `Monday; 0n], tue_sales: $[sum n * d_day_name = `Tuesday; sum sp * d_day_name = `Tuesday; 0n], wed_sales: $[sum n * d_day_name = `Wednesday; sum sp * d_day_name = `Wednesday; 0n], thu_sales: $[sum n * d_day_name = `Thursday; sum sp * d_day_name = `Thursday; 0n], fri_sales: $[sum n * d_day_name = `Friday; sum sp * d_day_name = `Friday; 0n], sat_sales: $[sum n * d_day_name = `Saturday; sum sp * d_day_name = `Saturday; 0n] by s_store_name, s_store_id from ((0! select sp: sum ss_sales_price, n: count i by s_store_sk: ss_store_sk, d_date_sk: ss_sold_date_sk from store_sales where ss_sold_date_sk in q43_d) lj (select first d_day_name by d_date_sk from date_dim)) lj store",
        ),
        (
            "Q50 return latency buckets",
            "q50_d2: exec d_date_sk from date_dim where d_year = 2001; \
             q50_tk: distinct exec sr_ticket_number from store_returns where sr_returned_date_sk in q50_d2, not null sr_customer_sk; \
             100 sublist select d30: sum diff <= 30, d31_60: sum (diff > 30) & diff <= 60, d60_plus: sum diff > 60 by s_store_name, s_state from ((select item_sk, ticket, s_store_sk, diff: sr_returned_date_sk - ss_sold_date_sk from (0! (select first sr_returned_date_sk, first sr_customer_sk by item_sk: sr_item_sk, ticket: sr_ticket_number from store_returns where sr_returned_date_sk in q50_d2, not null sr_customer_sk) ij (select first ss_sold_date_sk, first ss_customer_sk, s_store_sk: first ss_store_sk by item_sk: ss_item_sk, ticket: ss_ticket_number from store_sales where ss_ticket_number in q50_tk, not null ss_customer_sk)) where ss_customer_sk = sr_customer_sk) lj store)",
        ),
        (
            "Q52 brand sales for a manager",
            "q52_d: exec d_date_sk from date_dim where d_moy = 11, d_year = 2000; \
             q52_i: exec i_item_sk from item where i_manager_id = 1; \
             100 sublist `d_year xasc `ext_price xdesc `brand_id xasc 0! select ext_price: sum s_ext by d_year: 2000, brand_id: i_brand_id, brand: i_brand from (0! select s_ext: sum ss_ext_sales_price by i_item_sk: ss_item_sk from store_sales where ss_sold_date_sk in q52_d, ss_item_sk in q52_i) lj (select first i_brand_id, first i_brand by i_item_sk from item where i_item_sk in q52_i)",
        ),
        (
            "Q55 brand sales for manager 28",
            "q55_d: exec d_date_sk from date_dim where d_moy = 11, d_year = 1999; \
             q55_i: exec i_item_sk from item where i_manager_id = 28; \
             100 sublist `ext_price xdesc `brand_id xasc 0! select ext_price: sum s_ext by brand_id: i_brand_id, brand: i_brand from (0! select s_ext: sum ss_ext_sales_price by i_item_sk: ss_item_sk from store_sales where ss_sold_date_sk in q55_d, ss_item_sk in q55_i) lj (select first i_brand_id, first i_brand by i_item_sk from item where i_item_sk in q55_i)",
        ),
        (
            "Q69 store-only customers",
            "q69_d: exec d_date_sk from date_dim where d_year = 2001, d_moy within 4 6; \
             q69_a: exec ca_address_sk from customer_address where ca_state in `KY`GA`NM`TN`AL; \
             q69_ss: distinct exec ss_customer_sk from store_sales where ss_sold_date_sk in q69_d, not null ss_customer_sk; \
             q69_ws: distinct exec ws_bill_customer_sk from web_sales where ws_sold_date_sk in q69_d; \
             q69_cs: distinct exec cs_bill_customer_sk from catalog_sales where cs_sold_date_sk in q69_d; \
             100 sublist select cnt1: count i by cd_gender, cd_marital_status, cd_education_status from (select cd_demo_sk: c_current_cdemo_sk from customer where c_current_addr_sk in q69_a, c_customer_sk in q69_ss, not c_customer_sk in q69_ws, not c_customer_sk in q69_cs) lj customer_demographics",
        ),
        (
            "Q87 store-only buyers (EXCEPT)",
            "q87_d: exec d_date_sk from date_dim where d_month_seq within 1200 1211; \
             select cnt: count i from ((select distinct c_last_name, c_first_name, d_date from ((select distinct c_customer_sk: ss_customer_sk, d_date_sk: ss_sold_date_sk from store_sales where ss_sold_date_sk in q87_d, not null ss_customer_sk) ij (select first c_last_name, first c_first_name by c_customer_sk from customer)) lj (select first d_date by d_date_sk from date_dim)) lj ((select ok: 1b by c_last_name, c_first_name, d_date from ((select distinct c_customer_sk: cs_bill_customer_sk, d_date_sk: cs_sold_date_sk from catalog_sales where cs_sold_date_sk in q87_d) ij (select first c_last_name, first c_first_name by c_customer_sk from customer)) lj (select first d_date by d_date_sk from date_dim)) uj (select ok: 1b by c_last_name, c_first_name, d_date from ((select distinct c_customer_sk: ws_bill_customer_sk, d_date_sk: ws_sold_date_sk from web_sales where ws_sold_date_sk in q87_d) ij (select first c_last_name, first c_first_name by c_customer_sk from customer)) lj (select first d_date by d_date_sk from date_dim)))) where not ok",
        ),
        (
            "Q96 evening shoppers with 7 dependents",
            "select cnt: count i from store_sales where ss_sold_time_sk in (exec t_time_sk from time_dim where t_hour = 20, t_minute >= 30), ss_hdemo_sk in (exec hd_demo_sk from household_demographics where hd_dep_count = 7), ss_store_sk in (exec s_store_sk from store where s_store_name = `ese)",
        ),
        (
            "Q98 store revenue ratio per class",
            "q98_d: exec d_date_sk from date_dim where d_date within 1999.02.22 1999.03.24; \
             q98_i: exec i_item_sk from item where i_category in `Sports`Books`Home; \
             `i_category`i_class`i_item_id`revenueratio xasc update revenueratio: 100.0 * itemrevenue % (sum; itemrevenue) fby i_class from 0! select itemrevenue: sum rev by i_item_id, i_category, i_class, i_current_price from (0! select rev: sum ss_ext_sales_price by i_item_sk: ss_item_sk from store_sales where ss_sold_date_sk in q98_d, ss_item_sk in q98_i) lj (select first i_item_id, first i_category, first i_class, first i_current_price by i_item_sk from item where i_item_sk in q98_i)",
        ),
        (
            "Q11 year-over-year growth by channel",
            "q11_d1: exec d_date_sk from date_dim where d_year = 2001; \
             q11_d2: exec d_date_sk from date_dim where d_year = 2002; \
             100 sublist `c_customer_id xasc select c_customer_id from ((0! ((((select s1: sum ss_ext_sales_price by c_customer_sk: ss_customer_sk from store_sales where ss_sold_date_sk in q11_d1, not null ss_customer_sk) ij (select s2: sum ss_ext_sales_price by c_customer_sk: ss_customer_sk from store_sales where ss_sold_date_sk in q11_d2, not null ss_customer_sk)) ij (select w1: sum ws_ext_sales_price by c_customer_sk: ws_bill_customer_sk from web_sales where ws_sold_date_sk in q11_d1)) ij (select w2: sum ws_ext_sales_price by c_customer_sk: ws_bill_customer_sk from web_sales where ws_sold_date_sk in q11_d2))) ij (select first c_customer_id by c_customer_sk from customer)) where s1 > 0, w1 > 0, (w2 % w1) > s2 % s1",
        ),
        (
            "Q51 cumulative sales window",
            "q51_d: exec d_date_sk from date_dim where d_month_seq within 1200 1211; \
             100 sublist update cume_sales: sums daily by item_sk from 0! select daily: sum s_daily by item_sk, d_date from (0! select s_daily: sum ss_sales_price by item_sk: ss_item_sk, d_date_sk: ss_sold_date_sk from store_sales where ss_item_sk <= 20, ss_sold_date_sk in q51_d) lj (select first d_date by d_date_sk from date_dim)",
        ),
    ];
    v.into_iter().map(|(n, s)| (n.to_string(), s.to_string())).collect()
}

/// Recompute selected answers from the generated rows.
pub fn check(data: &Data, name: &str, rows: &[Vec<String>]) -> Option<Result<(), String>> {
    match name.split(' ').next().unwrap() {
        "Q3" => {
            let mut g: HashMap<(i32, i64, String), i64> = HashMap::new();
            for s in &data.sales {
                let it = &data.items[&s.item];
                let (y, m) = data.dates[&s.date];
                if it.manufact == 128 && m == 11 {
                    *g.entry((y, it.brand_id, it.brand.clone())).or_default() += s.ext;
                }
            }
            let shown = rows.len().min(100);
            if shown != g.len().min(100) {
                return Some(Err(format!("{} rows, expected {}", rows.len(), g.len().min(100))));
            }
            for r in rows {
                let key = (num(&r[0]) as i32, num(&r[1]) as i64, r[2].trim().to_string());
                match g.get(&key) {
                    Some(v) if close(num(&r[3]), *v as f64 / 100.0) => {}
                    other => return Some(Err(format!("row {r:?}: expected {other:?}"))),
                }
            }
            Some(Ok(()))
        }
        "Q42" => {
            let mut g: HashMap<(i32, i64, String), i64> = HashMap::new();
            for s in &data.sales {
                let it = &data.items[&s.item];
                let (y, m) = data.dates[&s.date];
                if it.manager == 1 && m == 11 && y == 2000 {
                    *g.entry((y, it.cat_id, it.cat.clone())).or_default() += s.ext;
                }
            }
            if rows.len() != g.len().min(100) {
                return Some(Err(format!("{} rows, expected {}", rows.len(), g.len().min(100))));
            }
            for r in rows {
                let key = (num(&r[0]) as i32, num(&r[1]) as i64, r[2].trim().to_string());
                match g.get(&key) {
                    Some(v) if close(num(&r[3]), *v as f64 / 100.0) => {}
                    other => return Some(Err(format!("row {r:?}: expected {other:?}"))),
                }
            }
            Some(Ok(()))
        }
        "Q96" => {
            let n = data
                .sales
                .iter()
                .filter(|s| s.time / 60 == 20 && s.time % 60 >= 30 && data.hd_dep[&s.hdemo] == 7 && data.store_names[&s.store] == "ese")
                .count();
            if rows.len() == 1 && num(&rows[0][0]) as usize == n {
                Some(Ok(()))
            } else {
                Some(Err(format!("count {rows:?} expected {n}")))
            }
        }
        _ => {
            let _ = |s: &Sale| s.qty;
            None
        }
    }
}
