use lib qw'../lib';
use TestConnector;
use SQL::Struct (connector => 'TestConnector');
use Test::More;

my ($query, $bind);
ok(defined SQL::Struct::connect('', '', ''), 'connected');
one_row("prim", 1);
($query, $bind) = TestConnector::query();
ok($query eq 'select * from prim where id = ?' && $bind eq "'1'",
	'select from primary key');
one_row("list", {id => 1});
($query, $bind) = TestConnector::query();
ok($query eq 'select * from list  WHERE ( id = ? )' && $bind eq "'1'",
	'select from field');
one_row(
	"list",
	-group_by => "ref",
	-having   => {"length(ref)" => {'>', 4}}
);
($query, $bind) = TestConnector::query();
ok( $query eq 'select * from list  GROUP BY "ref"  HAVING ( length(ref) > ? )'
	  && $bind eq "'4'",
	'select with GROUP BY and HAVING'
);
one_row(
	"list",
	1,
	-group_by => "ref",
	-having   => {"length(ref)" => {'>', 4}}
);
($query, $bind) = TestConnector::query();
ok( $query eq
	  'select * from list where id = ? GROUP BY "ref"  HAVING ( length(ref) > ? )'
	  && $bind eq "'1','4'",
	'select with ID, GROUP BY and HAVING'
);

my $list = one_row("list", 1);
ok($list && $list->ref eq 'reference1', 'select data');

done_testing();
