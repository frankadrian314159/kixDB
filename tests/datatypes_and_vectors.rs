use kixdb::{Attr, ColVec, Server, K};

#[test]
fn right_to_left_and_minus_disambiguation() {
    let srv = Server::new();
    let mut c = srv.connect();

    assert_eq!(c.eval("2 * 3 + 4").unwrap().to_string(), "14");
    assert_eq!(c.eval("10-3").unwrap().to_string(), "7");
    assert_eq!(c.eval("1 - 2").unwrap().to_string(), "-1");
    assert_eq!(c.eval("1 -2").unwrap().to_string(), "1 -2");
    assert_eq!(c.eval("sum 1 + til 10").unwrap().to_string(), "55");
}

#[test]
fn typed_nulls_infinities_and_temporals() {
    let srv = Server::new();
    let mut c = srv.connect();

    assert_eq!(c.eval("null 1 0N 3").unwrap().to_string(), "010b");
    assert_eq!(c.eval("10 ^ 1 0N 3").unwrap().to_string(), "1 10 3");
    assert_eq!(c.eval("fills 0N 10 0N 0N 20 0N").unwrap().to_string(), "0N 10 10 10 20 20");

    // Epoch 2000.01.01 = 0i
    assert_eq!(c.eval("`int$2000.01.01").unwrap().to_string(), "0i");
    assert_eq!(c.eval("2026.11.24 + 6").unwrap().to_string(), "2026.11.30");
    assert_eq!(c.eval("2026.11.30 - 2026.11.24").unwrap().to_string(), "6i");
    assert_eq!(c.eval("d: 2026.11.24 2026.12.01; d.mm").unwrap().to_string(), "11 12i");
    assert_eq!(c.eval("t: 09:30:15.500 09:35:45.000; 5 xbar t.minute").unwrap().to_string(), "09:30 09:35");
}

#[test]
fn temporal_nulls_and_infinities_read_back_as_they_print() {
    let srv = Server::new();
    let mut c = srv.connect();

    for t in ['p', 'm', 'd', 'z', 'n', 'u', 'v', 't'] {
        let list = format!("0N{t} 0W{t} -0W{t}");
        assert_eq!(c.eval(&list).unwrap().to_string(), list);
        assert_eq!(c.eval(&format!("x: {list}; null x")).unwrap().to_string(), "100b");
        assert_eq!(c.eval(&format!("-9!-8!{list}")).unwrap().to_string(), list);
    }
    assert_eq!(c.eval("(type 0Nz; type -0Wz; null 0N 2026.01.01T12)").unwrap().to_string(), "(-15h;-15h;10b)");
}

#[test]
fn an_hour_may_stand_alone_in_a_time_with_a_date_or_days() {
    let srv = Server::new();
    let mut c = srv.connect();

    for (short, long) in [
        ("2026.01.01D0", "2026.01.01D00:00:00"),
        ("2026.01.01D12 2026.01.01D7", "2026.01.01D12:00 2026.01.01D07:00"),
        ("2026.01.01T23", "2026.01.01T23:00"),
        ("0D0 1D12 -1D12", "0D00:00 1D12:00 -1D12:00"),
        ("\"P\"$\"2026.01.01D12\"", "2026.01.01D12:00"),
    ] {
        assert_eq!(c.eval(short).unwrap(), c.eval(long).unwrap(), "{short}");
    }
    assert!(c.eval("2026.01.01D123").is_err());
}

#[test]
fn temporal_infinities_stay_infinite_in_a_finer_type() {
    let srv = Server::new();
    let mut c = srv.connect();

    for (q, expected) in [
        ("00:00:01 0Wu -0Wu 0Nu", "00:00:01 0Wv -0Wv 0Nv"),
        ("0D1 0Wu -0Wt", "0D01:00:00.000000000 0Wn -0Wn"),
        ("2026.01.01D0 0Wd -0Wz", "2026.01.01D00:00:00.000000000 0Wp -0Wp"),
        ("2026.01.01T0 0Wd -0Wd", "2026.01.01T00:00:00.000 0Wz -0Wz"),
        ("`date$(0Wm;-0Wm;0Nm)", "0Wd -0Wd 0Nd"),
        ("(12:00:00<0Wu; 12:00:00>-0Wu; 2026.01.01<0Wm; 2026.01.01>-0Wm; .z.p<0Wd; .z.p>-0Wz)", "111111b"),
    ] {
        assert_eq!(c.eval(q).unwrap().to_string(), expected, "{q}");
    }
}

#[test]
fn casts_between_temporal_types_keep_nulls_and_infinities() {
    let srv = Server::new();
    let mut c = srv.connect();

    for (target, to) in [("timestamp", 'p'), ("month", 'm'), ("date", 'd'), ("datetime", 'z'), ("timespan", 'n'), ("minute", 'u'), ("second", 'v'), ("time", 't')] {
        let expected = format!("0N{to} 0W{to} -0W{to}");
        for from in ['p', 'm', 'd', 'z', 'n', 'u', 'v', 't'] {
            let list = format!("`{target}$0N{from} 0W{from} -0W{from}");
            assert_eq!(c.eval(&list).unwrap().to_string(), expected, "{list}");
            for (value, name) in [("0N", 0), ("0W", 1), ("-0W", 2)] {
                let atom = format!("`{target}${value}{from}");
                assert_eq!(c.eval(&atom).unwrap().to_string(), expected.split(' ').nth(name).unwrap(), "{atom}");
            }
        }
    }
    for (q, expected) in [
        ("`time$2026.01.01D12:00:00 0Wp", "12:00:00.000 0Wt"),
        ("(0Wp;2026.01.01D12).date", "0Wd 2026.01.01"),
        ("`timestamp$2026.02.03 1666.01.01 2500.01.01", "2026.02.03D00:00:00.000000000 -0Wp 0Wp"),
        ("`timestamp$2026.02m", "2026.02.01D00:00:00.000000000"),
        ("(`minute$0N; `second$0N; `time$0N)", "(0Nu;0Nv;0Nt)"),
        // (a number's infinity is only a large number, as in q)
        ("`int$0Wh", "32767i"),
    ] {
        assert_eq!(c.eval(q).unwrap().to_string(), expected, "{q}");
    }
}

#[test]
fn strings_are_read_as_timespans_and_datetimes() {
    let srv = Server::new();
    let mut c = srv.connect();

    for (text, value) in [
        ("\"N\"$(\"12:00\";\"01:02:03.5\";\"2D3\";\"-1D12\")", "0D12:00 0D01:02:03.5 2D03 -1D12"),
        ("\"N\"$(\"123456123987654\";\"123456\";\"0Wn\";\"-0Wu\")", "0D12:34:56.123987654 0D12:34:56 0Wn -0Wn"),
        ("\"N\"$(\"junk\";\"\";\"12\")", "0Nn 0Nn 0Nn"),
        ("\"Z\"$(\"2026-01-01 12:00:00.123\";\"2026.01.01T12\";\"2026.01.01\")", "2026.01.01T12:00:00.123 2026.01.01T12:00 2026.01.01T00"),
        ("\"Z\"$(\"20191122-11:11:11.123\";\"20191122\")", "2019.11.22T11:11:11.123 2019.11.22T00"),
        ("\"Z\"$(\"0Wz\";\"-0Wz\";\"junk\";\"2026\")", "0Wz -0Wz 0Nz 0Nz"),
        ("\"P\"$(\"2026.01.01\";\"20191122-11:11:11.123\";\"-0Wp\")", "2026.01.01D0 2019.11.22D11:11:11.123 -0Wp"),
        ("(\"NZ\";\",\")0:(\"12:00,2026-01-01 12:00\";\"junk,junk\")", "(0D12:00 0Nn; 2026.01.01T12:00 0Nz)"),
    ] {
        assert_eq!(c.eval(text).unwrap(), c.eval(value).unwrap(), "{text}");
    }
}

#[test]
fn attributes_and_validation() {
    let srv = Server::new();
    let mut c = srv.connect();

    let s = c.eval("`s#1 2 2 5").unwrap();
    if let K::Vector(cv) = &s {
        assert_eq!(cv.attr(), Attr::Sorted);
    } else {
        panic!("expected vector");
    }
    assert!(c.eval("`s#3 1 2").is_err());

    let u = c.eval("`u#`AAPL`MSFT`GOOG").unwrap();
    if let K::Vector(cv) = &u {
        assert_eq!(cv.attr(), Attr::Unique);
    } else {
        panic!("expected vector");
    }
    assert!(c.eval("`u#`AAPL`AAPL").is_err());

    let p = c.eval("`p#`AAPL`AAPL`MSFT`MSFT`GOOG").unwrap();
    if let K::Vector(cv) = &p {
        assert_eq!(cv.attr(), Attr::Parted);
    } else {
        panic!("expected vector");
    }
    assert!(c.eval("`p#`AAPL`MSFT`AAPL").is_err());
}

#[test]
fn lambdas_projections_adverbs_and_views() {
    let srv = Server::new();
    let mut c = srv.connect();

    // Lambdas & projections
    assert_eq!(c.eval("{x + y * z}[2; 3; 4]").unwrap().to_string(), "14");
    assert_eq!(c.eval("add10: {[a;b] a + b}[10;]; add10 25").unwrap().to_string(), "35");

    // Adverbs: over, scan, each, each-prior, each-left, each-right
    assert_eq!(c.eval("(+/) 1 2 3 4 5").unwrap().to_string(), "15");
    assert_eq!(c.eval("(+\\) 1 2 3 4").unwrap().to_string(), "1 3 6 10");
    assert_eq!(c.eval("(-':) 10 13 11 20").unwrap().to_string(), "10 3 -2 9");
    assert_eq!(c.eval("count' (\"hi\"; \"world\"; \"q\")").unwrap().to_string(), "2 5 1");
    assert_eq!(c.eval("10 +/: 1 2 3").unwrap().to_string(), "11 12 13");
    assert_eq!(c.eval("1 2 3 -\\: 10").unwrap().to_string(), "-9 -8 -7");

    // Iteration `n f/ init` (Fibonacci!)
    assert_eq!(c.eval("last 10 {x, sum -2#x}/ 0 1").unwrap().to_string(), "89");

    // Protected evaluation `@[f; x; handler]`
    assert_eq!(c.eval("@[{1 + x}; `bad; {\"caught:\", x}]").unwrap().as_str().unwrap(), "caught:type");

    // Views (`::`) recompute automatically when dependencies change!
    c.eval("base: 100; doubled:: base * 2").unwrap();
    assert_eq!(c.eval("doubled").unwrap().to_string(), "200");
    c.eval("base: 250").unwrap();
    assert_eq!(c.eval("doubled").unwrap().to_string(), "500");
}

#[test]
fn strings_casts_and_dictionaries() {
    let srv = Server::new();
    let mut c = srv.connect();

    assert_eq!(c.eval("\"I\"$\"42\"").unwrap().to_string(), "42i");
    assert_eq!(c.eval("`$\"AAPL\"").unwrap().to_string(), "`AAPL");
    assert_eq!(c.eval("`AAPL`MSFT`IBM like \"*M*\"").unwrap().to_string(), "011b");
    assert_eq!(c.eval("ssr[\"hello world\"; \"world\"; \"kixdb\"]").unwrap().as_str().unwrap(), "hello kixdb");
    assert_eq!(c.eval("\"|\" sv \",\" vs \"a,b,c\"").unwrap().as_str().unwrap(), "a|b|c");

    c.eval("d: `a`b`c ! 10 20 30; d[`b]+: 5").unwrap();
    assert_eq!(c.eval("d[`b]").unwrap().to_string(), "25");
    if let K::Vector(ColVec::Sym(_, _)) = c.eval("key d").unwrap() {} else { panic!("expected sym keys"); }
}
