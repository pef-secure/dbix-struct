use lib qw'../lib lib t';
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
ok($list && ref ($list->refPlAssocList) eq 'DBC::PlAssoc',
	'got back reference');
ok($list && ref ($list->refPlAssocList->Prim) eq 'DBC::Prim',
	'got back reference and direct reference');
ok($list && $list->refPlAssocList->Prim->payload eq 'pay1',
	'got data from associated table');
DBC::List->update({ref => 33}, {id => 1});
($query, $bind) = TestConnector::query();
ok($query eq 'update list set ref = ?  WHERE ( id = ? )' && $bind eq "'33','1'",
	'table update');
DBC::List->delete({id => 1});
($query, $bind) = TestConnector::query();
ok($query eq 'delete from list  WHERE ( id = ? )' && $bind eq "'1'",
	'table delete');
new_row("list", ref => 44);
($query, $bind) = TestConnector::query();
ok($query eq 'insert into list (ref) values (?) returning id' && $bind eq "'44'",
	'table insert');
#print "query: $query\nbind: $bind\n";
done_testing();
