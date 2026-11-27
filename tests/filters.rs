//! A query reads the rows its `where` keeps where they are, when it keeps most of them: what it gives is what the same
//! query gives of a table that holds just those rows.
use kixdb::Server;

#[test]
fn queries_of_kept_rows_match_queries_of_those_rows_alone() {
    let server = Server::new();
    let mut c = server.connect();
    // (more rows than are worked on by one thread; `j` is each row's number)
    c.eval("n: 300000; t: ([] g: n # `x`y`z; h: (n # 1 2) ; s: n # (\"ab\"; \"cd\"; \"abc\"); n: til n; p: 0.25 + (til n) mod 977; j: til n)").unwrap();
    // each `where`, and the same rows as the items of one test of the whole table
    let wheres = [
        ("n > 5", "t[`n] > 5"),
        ("n < 5000", "t[`n] < 5000"),
        ("n > 5, p < 900.0", "(t[`n] > 5) & t[`p] < 900.0"),
        ("g <> `x, h = 1, n within 10 200000", "(t[`g] <> `x) & (t[`h] = 1) & t[`n] within 10 200000"),
        ("not s like \"a*\", p > 3.0", "(not t[`s] like \"a*\") & t[`p] > 3.0"),
        ("p > 1.0, n > (avg; n) fby g", "(t[`p] > 1.0) & t[`n] > ((avg; t`n) fby t`g) "),
        ("n > 100, 0 = n mod 2, p > 2.0", "(t[`n] > 100) & (0 = t[`n] mod 2) & t[`p] > 2.0"),
        ("n < 0", "t[`n] < 0"),
    ];
    let queries = [
        "select total: sum p, mean: avg p, low: min n, high: max p, rows: count i, f: first s by g from T",
        "select w: sum p * 1 - n mod 3, q: avg n, d: dev p by g, h from T",
        "select mean: avg p by h from T",
        "select n by g from T",
        "select last p, a: avg j, low: min j by s from T",
        "select spread: (max p) - min p, n: count distinct h by g from T",
        "select sum p by b: n mod 3 from T",
        "select sum p by g: h from T",
        "select c: count i by g from T",
        "select sum p, avg n from T",
        "select g, p from T",
        "exec sum p by g from T",
    ];
    for (clauses, mask) in wheres {
        // the second test of `fby` sees only the rows the first clause kept
        if clauses.contains("fby") {
            c.eval("u: t where t[`p] > 1.0; u: u where u[`n] > (avg; u`n) fby u`g").unwrap();
        } else {
            c.eval(&format!("u: t where {mask}")).unwrap();
        }
        for query in queries {
            let filtered = c.eval(&query.replace('T', &format!("t where {clauses}")));
            let alone = c.eval(&query.replace('T', "u"));
            assert_eq!(filtered, alone, "{query} where {clauses}");
        }
        // `i` is a row's number in the whole table, however the rows are read
        let numbers = c.eval(&format!("select a: min i, b: max i by g from t where {clauses}")).unwrap();
        assert_eq!(numbers, c.eval("select a: min j, b: max j by g from u").unwrap(), "i where {clauses}");
    }
}
