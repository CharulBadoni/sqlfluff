
select a+b sum, col_C, sum(col_B) From
foo jOIN moo on foo.col=moo.col join
boo on foo.col=boo.col where col_A<>1
group by 1, col_C Order bY sum, 2
