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
