package DBIx::Struct::Connector;
use strict;
use warnings;
use base 'DBIx::Connector';

our $db_reconnect_timeout = 30;

sub _connect {
	my ($self, @args) = @_;
	for my $try (1 .. $db_reconnect_timeout) {
		my $dbh = eval { $self->SUPER::_connect(@args) };
		return $dbh if $dbh;
		sleep 1 if $try != $db_reconnect_timeout;
	}
	die $@ if $@;
	die "DB connect error";
}

package DBIx::Struct::Error::String;
use strict;
use warnings;
use Carp;

sub error_message (+%) {
	my $msg = $_[0];
	delete $msg->{result};
	my $message = delete $msg->{message};
	croak join "; ", $message, map { "$_: $msg->{$_}" } keys %$msg;
}

package DBIx::Struct::Error::Hash;
use strict;
use warnings;

sub error_message (+%) {
	die $_[0];
}

package DBIx::Struct;
use strict;
use warnings;
use SQL::Abstract;
use Digest::MD5;
use Data::Dumper;
use base 'Exporter';
use v5.10;

our $VERSION = '0.02';

our @EXPORT = qw{
  one_row
  all_rows
  new_row
};

our @EXPORT_OK = qw{
  connector
  hash_ref_slice
};

our $conn;
our $update_on_destroy     = 1;
our $connector_module      = 'DBIx::Struct::Connector';
our $connector_constructor = 'new';
our $connector_args        = [];
our $connector_driver;
our $table_classes_namespace = 'DBC';
our $query_classes_namespace = 'DBQ';
our $error_message_class     = 'DBIx::Struct::Error::String';
our %driver_pk_insert;

sub error_message (+%);

%driver_pk_insert = (
	_returning => sub {
		my ($table, $pk_row_data, $pk_returninig) = @_;
		my $ret;
		if ($pk_row_data) {
			$ret = <<INS;
						($pk_row_data) =
							\$_->selectrow_array(\$insert . " $pk_returninig", undef, \@bind)
INS
		} else {
			$ret = <<INS;
						\$_->do(\$insert, undef, \@bind)
INS
		}
		$ret .= <<INS
						or DBIx::Struct::error_message {
							result  => 'SQLERR',
							message => 'error '.\$_->errstr.' inserting into table $table'
						};
INS
	},
	_last_id_undef => sub {
		my ($table, $insert_exp, $pk_row_data) = @_;
		my $ret;
		$ret = <<INS;
						\$_->do(\$insert, undef, \@bind)
						or DBIx::Struct::error_message {
							result  => 'SQLERR',
							message => 'error '.\$_->errstr.' inserting into table $table'
						};
INS
		if ($pk_row_data) {
			$ret .= <<INS;
						$pk_row_data = $_->last_insert_id(undef, undef, undef, undef);
INS
		}
	},
	_last_id_empty => sub {
		my ($table, $insert_exp, $pk_row_data) = @_;
		my $ret;
		$ret = <<INS;
						\$_->do(\$insert, undef, \@bind)
						or DBIx::Struct::error_message {
							result  => 'SQLERR',
							message => 'error '.\$_->errstr.' inserting into table $table'
						};
INS
		if ($pk_row_data) {
			$ret .= <<INS;
						$pk_row_data = $_->last_insert_id("", "", "", "");
INS
		}
	}
);

$driver_pk_insert{Pg}     = $driver_pk_insert{_returning};
$driver_pk_insert{mysql}  = $driver_pk_insert{_last_id_undef};
$driver_pk_insert{SQLite} = $driver_pk_insert{_last_id_empty};

sub hash_ref_slice($@) {
	my ($hashref, @slice) = @_;
	error_message {
		message => "first parameter is not hash reference",
		result  => 'INTERR',
	  }
	  if 'HASH' ne ref $hashref;
	map { $_ => $hashref->{$_} } @slice;
}

sub check_package_scalar {
	my ($package, $scalar) = @_;
	no strict 'refs';
	my $pr = \%{$package . '::'};
	my $er = $$pr{$scalar};
	return unless $er;
	defined *{$er}{'SCALAR'};
}

sub import {
	my ($class, @args) = @_;
	state $init_import = 0;
	my $defconn = 0;
	for (my $i = 0 ; $i < @args ; ++$i) {
		if ($args[$i] eq 'connector_module') {
			(undef, $connector_module) = splice @args, $i, 2;
			--$i;
			if (not $defconn and check_package_scalar($connector_module, 'conn')) {
				no strict 'refs';
				*conn = \${$connector_module . '::conn'};
			}
		} elsif ($args[$i] eq 'connector_constructor') {
			(undef, $connector_constructor) = splice @args, $i, 2;
			--$i;
		} elsif ($args[$i] eq 'table_classes_namespace') {
			(undef, $table_classes_namespace) = splice @args, $i, 2;
			--$i;
		} elsif ($args[$i] eq 'query_classes_namespace') {
			(undef, $query_classes_namespace) = splice @args, $i, 2;
			--$i;
		} elsif ($args[$i] eq 'connect_timeout') {
			(undef, $db_reconnect_timeout) = splice @args, $i, 2;
			--$i;
		} elsif ($args[$i] eq 'error_class') {
			my (undef, $emc) = splice @args, $i, 2;
			$error_message_class = $emc if !$init_import;
			--$i;
		} elsif ($args[$i] eq 'connector_args') {
			(undef, $connector_args) = splice @args, $i, 2;
			--$i;
		} elsif ($args[$i] eq 'connector_object') {
			$defconn = 1;
			my (undef, $connector_object) = splice @args, $i, 2;
			--$i;
			*conn = \${$connector_object};
		}
	}
	if (!$init_import) {
		my $eval = "*error_message = \\&$error_message_class" . "::error_message";
		eval $eval;
	}
	my %imps = map { $_ => undef } @args, @EXPORT;
	$class->export_to_level(1, $class, keys %imps);
	$init_import = 1;
}

sub connector {
	$conn;
}

sub _not_yet_connected {
	if (not $conn) {
		my ($dsn, $user, $password) = @_;
		if ($dsn && $dsn !~ /^dbi:/i) {
			$dsn = "dbi:Pg:dbname=$dsn";
		}
		my $connect_attrs = {
			AutoCommit          => 1,
			PrintError          => 0,
			AutoInactiveDestroy => 1,
			RaiseError          => 0,
		};
		if ($dsn) {
			my ($driver) = $dsn =~ /^dbi:(\w*?)(?:\((.*?)\))?:/i;
			if ($driver) {
				if ($driver eq 'Pg') {
					$connect_attrs->{pg_enable_utf8} = 1;
				} elsif ($driver eq 'mysql') {
					$connect_attrs->{mysql_enable_utf8} = 1;
				}
			}
		}
		if (!@$connector_args) {
			@$connector_args = ($dsn, $user, $password, $connect_attrs);
		}
		$conn = $connector_module->$connector_constructor(@$connector_args)
		  or error_message {
			message => "DB connect error",
			result  => 'SQLERR',
		  };
		$conn->mode('fixup');
	}
	'' =~ /()/;
	$connector_driver = $conn->driver->{driver};
	no warnings 'redefine';
	*connect = \&connector;
	populate();
	$conn;
}

sub connect {
	goto &_not_yet_connected;
}

{
	my $md5 = Digest::MD5->new;

	sub make_name {
		my ($table) = @_;
		my $simple_table = (index ($table, " ") == -1);
		my $ncn;
		if ($simple_table) {
			$ncn = $table_classes_namespace . "::" . join ('', map { ucfirst ($_) } split (/[^a-zA-Z0-9]/, $table));
		} else {
			$md5->add($table);
			$ncn = $query_classes_namespace . "::" . "G" . $md5->hexdigest;
			$md5->reset;
		}
		$ncn;
	}
}

sub populate {
	my @tables;
	DBIx::Struct::connect->run(
		sub {
			my $sth = $_->table_info('', '', '%', "TABLE");
			return if not $sth;
			my $tables = $sth->fetchall_arrayref;
			@tables =
			  map { $_->[2] } grep { $_->[3] eq 'TABLE' and $_->[2] !~ /^sql_/ } @$tables;
		}
	);
	setup_row($_) for @tables;
}

sub _row_data ()    { 0 }
sub _row_updates () { 1 }

sub make_object_new {
	my ($table, $required, $pk_row_data, $pk_returninig) = @_;
	my $new = <<NEW;
		sub new {
			my \$class = \$_[0];
			my \$self = [ [] ];
			if(CORE::defined(\$_[1]) && ref(\$_[1]) eq 'ARRAY') {
				\$self->[@{[_row_data]}] = \$_[1];
			}
NEW
	if (not ref $table) {
		$new .= <<NEW;
			 elsif(CORE::defined \$_[1]) {
				my \%insert;
				for(my \$i = 1; \$i < \@_; \$i += 2) {
					if (CORE::exists \$fields{\$_[\$i]}) {
						\$self->[@{[_row_data]}]->[\$fields{\$_[\$i]}] = \$_[\$i + 1];
						\$insert{\$_[\$i]} = \$_[\$i + 1];
					} else {
						DBIx::Struct::error_message {
							result  => 'SQLERR',
							message => "unknown column \$_[\$i] inserting into table $table"
						}
					}
				}
				my (\@insert, \@values, \@bind);
				\@insert =
					CORE::map { 
						if(CORE::ref(\$insert{\$_}) eq 'ARRAY' and CORE::ref(\$insert{\$_}[0]) eq 'SCALAR') {
							CORE::push \@bind, \@{\$insert{\$_}}[1..\$#{\$insert{\$_}}];
							CORE::push \@values, \${\$insert{\$_}[0]};
							"\$_";
						} elsif(CORE::ref(\$insert{\$_}) eq 'SCALAR') {
							CORE::push \@values, \${\$insert{\$_}};
							"\$_";
						} else {
							CORE::push \@bind, \$insert{\$_};
							CORE::push \@values, "?";
							"\$_" 
						}						
					} keys \%insert;
				my \$insert = "insert into $table (" . CORE::join( ", ", \@insert) . ") values ("
					.  CORE::join( ", ", \@values) . ")";
NEW
		if ($required) {
			$new .= <<NEW;
				for my \$r ($required) {
					DBIx::Struct::error_message {
						result  => 'SQLERR',
						message => "required field \$r is absent for table $table"
					} if not CORE::exists \$insert{\$r};
				}
NEW
		}
		$new .= <<NEW;
				DBIx::Struct::connect->run(
					sub {
NEW
		$new .= $driver_pk_insert{$connector_driver}->($table, $pk_row_data, $pk_returninig);
		$new .= <<NEW;
	  			});
			}
NEW
	}
	$new .= <<NEW;
	  		bless \$self, \$class;
		}
NEW
	$new;
}

sub make_object_filter_timestamp {
	my ($timestamps) = @_;
	my $filter_timestamp = <<FTS;
		sub filter_timestamp {
			my \$self = \$_[0];
			if(\@_ == 1) {
				for my \$f ($timestamps) {
					\$self->[@{[_row_data]}][\$fields{\$f}] =~ s/\\.\\d+\$// if \$self->[@{[_row_data]}][\$fields{\$f}];
				}
			} else {
				for my \$f (\@_[1..\$#_]) {
					\$self->[@{[_row_data]}][\$fields{\$f}] =~ s/\\.\\d+\$// if \$self->[@{[_row_data]}][\$fields{\$f}];
				}
			}
			'' =~ /()/;
			\$self;
		}
FTS
	$filter_timestamp;
}

sub make_object_set {
	my $set = <<SET;
		sub set {
			my \$self = \$_[0];
			if(CORE::defined(\$_[1])) {
				if(ref(\$_[1]) eq 'ARRAY') {
					\$self->[@{[_row_data]}] = \$_[1];
					\$self->[@{[_row_updates]}] = {};
				} elsif(ref(\$_[1]) eq 'HASH') {
					for my \$f (keys \%{\$_[1]}) {
						if (CORE::exists \$fields{\$_[\$f]}) {
							\$self->[@{[_row_data]}]->[\$fields{\$f}] = \$_[1]->{\$f};
							\$self->[@{[_row_updates]}]{\$f} = undef;
						}
					}
				} elsif(not ref(\$_[1])) {
					for(my \$i = 1; \$i < \@_; \$i += 2) {
						if (CORE::exists \$fields{\$_[\$i]}) {
							\$self->[@{[_row_data]}]->[\$fields{\$_[\$i]}] = \$_[\$i + 1];
							\$self->[@{[_row_updates]}]{\$_[\$i]} = undef;
						}
					}
				}
			}
			\$self;
		}
SET
	$set;
}

sub make_object_data {
	my $data = <<DATA;
		sub data {
			my \$self = \$_[0];
			my \@ret_keys;
			my \$ret;
			if(CORE::defined(\$_[1])) {
				if(CORE::ref(\$_[1]) eq 'ARRAY') {
					if(!\@{\$_[1]}) {
						\$ret = \$self->[@{[_row_data]}];
					} else {
						\$ret = [CORE::map {\$self->[@{[_row_data]}]->[\$fields{\$_}] } CORE::grep {CORE::exists \$fields{\$_}} \@{\$_[1]}];
					}
				} else {
					for my \$k (\@_[1..\$#_]) {
						CORE::push \@ret_keys, \$k if CORE::exists \$fields{\$k};
					}
				}
			} else {
				\@ret_keys = keys \%fields;
			}
			\$ret = { CORE::map {\$_ => \$self->[@{[_row_data]}]->[\$fields{\$_}] } \@ret_keys} if not CORE::defined \$ret;
			\$ret;
		}
DATA
	$data;
}

sub make_object_update {
	my ($table, $pk_where, $pk_row_data) = @_;
	my $update;
	if (not ref $table) {
		# means this is just one simple table
		$update = <<UPD;
		sub update {
			my \$self = \$_[0];
			if(\@_ > 1 && ref(\$_[1]) eq 'HASH') {
				my (\$set, \$where, \@bind, \@bind_where);
				{
					no strict 'vars';
					local *set_hash = \$_[1];
					my \@unknown_columns = CORE::grep {not CORE::exists \$fields{\$_}} keys %set_hash;
					DBIx::Struct::error_message {
							result  => 'SQLERR',
							message => 'unknown columns '.CORE::join(", ", \@unknown_columns).' updating table $table'
					} if \@unknown_columns;
					\$set = 
						CORE::join ", ", 
						CORE::map { 
							if(CORE::ref(\$set_hash{\$_}) eq 'ARRAY' and CORE::ref(\$set_hash{\$_}[0]) eq 'SCALAR') {
								CORE::push \@bind, \@{\$set_hash{\$_}}[1..\$#{\$set_hash{\$_}}];
								"\$_ = " . \${\$set_hash{\$_}[0]};
							} elsif(CORE::ref(\$set_hash{\$_}) eq 'SCALAR') {
								"\$_ = " . \${\$set_hash{\$_}};
							} else {
								CORE::push \@bind, \$set_hash{\$_};
								"\$_ = ?" 
							}						
						} keys \%set_hash;
				}
				if(\@_ > 2) {
					my \$cond = \$_[2];
					if(not CORE::ref(\$cond)) {
						\$cond = {(selectKeys)[0] => \$_[2]};
					}
					(\$where, \@bind_where) = SQL::Abstract->new->where(\$cond);
				}
				DBIx::Struct::connect->run(sub {
					\$_->do(qq{update $table set \$set \$where}, undef, \@bind, \@bind_where)
					or DBIx::Struct::error_message {
						result  => 'SQLERR',
						message => 'error '.\$_->errstr.' updating table $table'
					}
				});
			} elsif (\@\$self > 1 && \%{\$self->[@{[_row_updates]}]}) {
				my (\$set, \@bind);
				{
					no strict 'vars';
					\$set = 
						CORE::join ", ", 
						CORE::map { 
							local *column_value = \\\$self->[@{[_row_data]}][\$fields{\$_}];
							if(CORE::ref(\$column_value) eq 'ARRAY' and CORE::ref(\$column_value->[0]) eq 'SCALAR') {
								CORE::push \@bind, \@{\$column_value}[1..\$#\$column_value];
								"\$_ = " . \${\$column_value->[0]};
							} elsif(CORE::ref(\$column_value) eq 'SCALAR') {
								"\$_ = " . \$\$column_value;
							} else {
								CORE::push \@bind, \$column_value;
								"\$_ = ?" 
							}						
						} keys \%{\$self->[@{[_row_updates]}]};
				}
				DBIx::Struct::connect->run(
					sub {
						\$_->do(qq{update $table set \$set where $pk_where}, undef, \@bind, $pk_row_data)
						or DBIx::Struct::error_message {
							result  => 'SQLERR',
							message => 'error '.\$_->errstr.' updating table $table'
						}
					}
				);
				pop \@{\$self};
			}
			\$self;
		}
UPD
	} else {
		$update = <<UPD;
		sub update {}
UPD
	}
	$update;
}

sub make_object_delete {
	my ($table, $pk_where, $pk_row_data) = @_;
	my $delete;
	if (not ref $table) {
		$delete = <<DEL;
		sub delete {
			my \$self = \$_[0];
			if(\@_ > 1) {
				my (\$where, \@bind);
				my \$cond = \$_[1];
				if(not CORE::ref(\$cond)) {
					\$cond = {(selectKeys)[0] => \$_[1]};
				}
				(\$where, \@bind) = SQL::Abstract->new->where(\$cond);
				DBIx::Struct::connect->run(sub {
					\$_->do(qq{delete from $table \$where}, undef, \@bind)
					or DBIx::Struct::error_message {
						result  => 'SQLERR',
						message => 'error '.\$_->errstr.' updating table $table'
					}
				});
			} else {
				DBIx::Struct::connect->run(
					sub {
						\$_->do(qq{delete from $table where $pk_where}, undef, $pk_row_data)
						or DBIx::Struct::error_message {
							result  => 'SQLERR',
							message => 'error '.\$_->errstr.' updating table $table'
						}
					});
			}
			\$self;
		}
DEL
	} else {
		$delete = <<DEL
		sub delete {}
DEL
	}
	$delete;
}

sub make_object_fetch {
	my ($table, $pk_where, $pk_row_data) = @_;
	my $fetch;
	if (not ref $table) {
		$fetch = <<FETCH;
		sub fetch {
			my \$self = \$_[0];
			if(\@_ > 1) {
				my (\$where, \@bind);
				my \$cond = \$_[1];
				if(not ref(\$cond)) {
					\$cond = {(selectKeys)[0] => \$_[1]};
				}
				(\$where, \@bind) = SQL::Abstract->new->where(\$cond);
				DBIx::Struct::connect->run(sub {
					\@{\$self->[@{[_row_data]}]} = \$_->selectrow_array(qq{select * from $table \$where}, undef, \@bind)
					or DBIx::Struct::error_message {
						result  => 'SQLERR',
						message => 'error '.\$_->errstr.' fetching table $table'
					}
				});
			} else {
				DBIx::Struct::connect->run(
					sub {
						\@{\$self->[@{[_row_data]}]} = \$_->selectrow_array(qq{select *  from $table where $pk_where}, undef, $pk_row_data)
						or DBIx::Struct::error_message {
							result  => 'SQLERR',
							message => 'error '.\$_->errstr.' fetching table $table'
						}
					});
			}
			\$self;
		}
FETCH
	} else {
		$fetch = <<FETCH;
		sub fetch { \$_[0] }
FETCH
	}
	$fetch;
}

sub setup_row {
	my ($table, $ncn) = @_;
	my $conn = DBIx::Struct::connect;
	error_message {
		result  => 'SQLERR',
		message => "Unsupported driver $connector_driver",
	  }
	  unless exists $driver_pk_insert{$connector_driver};
	$ncn ||= make_name($table);
	no strict "refs";
	if (grep { !/::$/ } keys %{"${ncn}::"}) {
		return $ncn;
	}
	my %fields;
	my @fields;
	my @timestamp_fields;
	my @required;
	my @pkeys;
	my @fkeys;
	my @refkeys;
	if (not ref $table) {
		# means this is just one simple table
		$conn->run(
			sub {
				my $cih = $_->column_info(undef, undef, $table, undef);
				error_message {
					result  => 'SQLERR',
					message => "Unknown table $table",
				  }
				  if not $cih;
				my $i = 0;
				while (my $chr = $cih->fetchrow_hashref) {
					$chr->{COLUMN_NAME} =~ s/"//g;
					push @fields, $chr->{COLUMN_NAME};
					if ($chr->{TYPE_NAME} =~ /timestamp/) {
						push @timestamp_fields, $chr->{COLUMN_NAME};
					}
					if ($chr->{NULLABLE} == 0 && !defined ($chr->{COLUMN_DEF})) {
						push @required, $chr->{COLUMN_NAME};
					}
					$fields{$chr->{COLUMN_NAME}} = $i++;
				}
				@pkeys = $_->primary_key(undef, undef, $table);
				my $sth = $_->foreign_key_info(undef, undef, undef, undef, undef, $table);
				if ($sth) {
					@fkeys =
					  grep { $_->{FK_COLUMN_NAME} !~ /[^a-z_0-9]/ } @{$sth->fetchall_arrayref({})};
				}
				$sth = $_->foreign_key_info(undef, undef, $table, undef, undef, undef);
				if ($sth) {
					@refkeys =
					  grep { $_->{FK_COLUMN_NAME} !~ /[^a-z_0-9]/ } @{$sth->fetchall_arrayref({})};
				}
			}
		);
	} else {
		# means this is a query
		%fields = %{$table->{NAME_hash}};
		$conn->run(
			sub {
				for (my $cn = 0 ; $cn < @{$table->{NAME}} ; ++$cn) {
					my $ti = $_->type_info($table->{TYPE}->[$cn]);
					push @timestamp_fields, $table->{NAME}->[$cn]
					  if $ti && $ti->{TYPE_NAME} =~ /timestamp/;
				}
			}
		);
	}
	my $fields = join ", ", map { qq|"$_" => $fields{$_}| } keys %fields;
	my $required = '';
	if (@required) {
		$required = join (", ", map { qq|"$_"| } @required);
	}
	my $timestamps = '';
	if (@timestamp_fields) {
		$timestamps = join (", ", map { qq|"$_"| } @timestamp_fields);
	} else {
		$timestamps = "()";
	}
	my %keywords = (
		new              => undef,
		set              => undef,
		data             => undef,
		delete           => undef,
		fetch            => undef,
		update           => undef,
		DESTROY          => undef,
		filter_timestamp => undef,
	);
	my $pk_row_data   = '';
	my $pk_returninig = '';
	my $pk_where      = '';
	my $select_keys   = '';
	my %pk_fields;
	if (@pkeys) {
		@pk_fields{@pkeys} = undef;
		$pk_row_data = join (", ", map { qq|\$self->[@{[_row_data]}]->[$fields{"$_"}]| } @pkeys);
		$pk_returninig = 'returning ' . join (", ", @pkeys);
		$pk_where = join (" and ", map { "$_ = ?" } @pkeys);
		my $sk_list = join (", ", map { qq|"$_"| } @pkeys);
		$select_keys = <<SK;
		sub selectKeys () { 
		 	($sk_list) 
		}
SK
	} else {
		if (@fields) {
			my $sk_list = join (", ", map { qq|"$_"| } @fields);
			$select_keys = <<SK;
		sub selectKeys () { 
			($sk_list)
		}
SK
		} else {
			$select_keys = <<SK;
		sub selectKeys () { () } 
SK
		}
	}
	my $foreign_tables = '';
	my %foreign_tables;
	for my $fk (@fkeys) {
		$fk->{FK_COLUMN_NAME} =~ s/"//g;
		my $fn = $fk->{FK_COLUMN_NAME};
		$fn =~ s/^id_// or $fn =~ s/_id(?=[^a-z]|$)//;
		$fn =~ tr/_/-/;
		$fn =~ s/\b(\w)/\u$1/g;
		$fn =~ tr/-//d;
		(my $pt = $fk->{PKTABLE_NAME}  || $fk->{UK_TABLE_NAME}) =~ s/"//g;
		(my $pk = $fk->{PKCOLUMN_NAME} || $fk->{UK_COLUMN_NAME}) =~ s/"//g;
		$foreign_tables .= <<FKT;
		sub $fn { 
			if(CORE::defined(\$_[0]->$fk->{FK_COLUMN_NAME})) {
				return DBIx::Struct::one_row("$pt", {$pk => \$_[0]->$fk->{FK_COLUMN_NAME}});
			} else { 
				return 
			} 
		}
FKT
		$foreign_tables{$pt} = [$fk->{FK_COLUMN_NAME} => $pk];
	}
	for my $ft (keys %foreign_tables) {
		my $ucft = ucfirst $ft;
		$foreign_tables .= <<FKT;
		sub foreignKey$ucft () {("$foreign_tables{$ft}[0]" => "$foreign_tables{$ft}[1]")}
FKT
	}
	my $references_tables = '';
	for my $rk (@refkeys) {
		$rk->{FK_TABLE_NAME} =~ s/"//g;
		my $ft = $rk->{FK_TABLE_NAME};
		(my $fk = $rk->{FK_COLUMN_NAME}) =~ s/"//g;
		(my $pt = $rk->{PKTABLE_NAME} || $rk->{UK_TABLE_NAME}) =~ s/"//g;
		(my $pk = $rk->{PKCOLUMN_NAME} || $rk->{UK_COLUMN_NAME}) =~ s/"//g;
		if ($pk ne $fk) {
			my $fn = $fk;
			$fn =~ s/^id_// or $fn =~ s/_id(?=[^a-z]|$)//;
			$fn =~ s/$ft//;
			$ft .= "_$fn" if $fn;
		}
		$ft =~ tr/_/-/;
		$ft =~ s/\b(\w)/\u$1/g;
		$ft =~ tr/-//d;
		$references_tables .= <<RT;
		sub ref${ft}s {
			my (\$self, \@cond) = \@_;
			my \%cond;
			if(\@cond) {
				if(not CORE::ref \$cond[0]) {
					\%cond = \@cond;
				} else {
					\%cond = \%{\$cond[0]};
				}
			}
			\$cond{$fk} = \$self->$pk;
			return DBIx::Struct::all_rows("$rk->{FK_TABLE_NAME}", \\\%cond);
		}
		sub ref${ft} {
			my (\$self, \@cond) = \@_;
			my \%cond;
			if(\@cond) {
				if(not CORE::ref \$cond[0]) {
					\%cond = \@cond;
				} else {
					\%cond = \%{\$cond[0]};
				}
			}
			\$cond{$fk} = \$self->$pk;
			return DBIx::Struct::one_row("$rk->{FK_TABLE_NAME}", \\\%cond);
		}
RT
	}
	my $accessors = '';
	for my $k (keys %fields) {
		next if exists $keywords{$k};
		$k =~ s/[^\w\d]/_/g;
		if (!exists ($pk_fields{$k}) && (not ref $table)) {
			$accessors .= <<ACC;
		sub $k {
			if(\@_ > 1) {
				\$_[0]->[@{[_row_data]}]->[$fields{$k}] = \$_[1];
				\$_[0]->[@{[_row_updates]}]{"$k"} = undef;
			}
			\$_[0]->[@{[_row_data]}]->[$fields{$k}];
		}
ACC
		} else {
			$accessors .= <<ACC;
		sub $k {
			\$_[0]->[@{[_row_data]}]->[$fields{$k}];
		}
ACC
		}

	}
	my $package_header = <<PHD;
		package ${ncn};
		use strict;
		use warnings;
		use Carp;
		use SQL::Abstract;
		our \%fields = ($fields);
PHD
	if (not ref $table) {
		$package_header .= <<PHD;
		our \$table_name = "$table";
PHD
	}
	my $new              = make_object_new($table, $required, $pk_row_data, $pk_returninig);
	my $filter_timestamp = make_object_filter_timestamp($timestamps);
	my $set              = make_object_set();
	my $data             = make_object_data();
	my $update           = make_object_update($table, $pk_where, $pk_row_data);
	my $delete           = make_object_delete($table, $pk_where, $pk_row_data);
	my $fetch            = make_object_fetch($table, $pk_where, $pk_row_data);
	my $destroy;
	if (not ref $table) {
		$destroy = <<DESTROY;
		sub DESTROY {
			no warnings 'once';
			\$_[0]->update if \$DBIx::Struct::update_on_destroy;
		}
DESTROY
	} else {
		$destroy = '';
	}
	my $eval_code = join "", $package_header, $select_keys, $new,
	  $filter_timestamp, $set, $data, $fetch,
	  $update, $delete, $destroy, $accessors, $foreign_tables,
	  $references_tables;
	#print $eval_code;
	eval $eval_code;
	'' =~ /()/;
	return $ncn;
}

sub _populated_table {
	my $ncn = $_[0];
	no strict "refs";
	if (grep { !/::$/ } keys %{"${ncn}::"}) {
		return 1;
	}
	return;
}

sub _table_name()    { 0 }
sub _table_alias()   { 1 }
sub _table_join()    { 2 }
sub _table_join_on() { 3 }

my $sql_abstract = SQL::Abstract->new;
my $tblnum;

sub _build_complex_query {
	my ($table, $query_bind, $where) = @_;
	return $table if not ref $table;
	my @from;
	my @columns;
	my @linked_list = (
		ref ($table) eq 'ARRAY'
		? @$table
		: error_message {
			result  => 'SQLERR',
			message => "Unsupported type of query: " . ref ($table)
		}
	);
	my ($conditions, $groupby, $having, $limit, $offset);
	for (my $i = 0 ; $i < @linked_list ; ++$i) {
		my $le = $linked_list[$i];
		if ('ARRAY' eq ref $le) {
			my $subfrom = _build_complex_query($le, $query_bind);
			my $ta = "t$tblnum";
			++$tblnum;
			push @from, ["($subfrom)", $ta];
			next;
		}
		if (substr ($le, 0, 1) ne '-') {
			my ($tn, $ta) = split ' ', $le;
			$ta = $tn if not $ta;
			error_message {
				result  => 'SQLERR',
				message => "Unknown table $tn"
			  }
			  unless _populated_table(make_name($tn));
			push @from, [$tn, $ta];
		} else {
			my $cmd = substr ($le, 1);
			if ($cmd eq 'left') {
				$from[-1][_table_join] = 'left join';
			} elsif ($cmd eq 'right') {
				$from[-1][_table_join] = 'right join';
			} elsif ($cmd eq 'join') {
				$from[-1][_table_join] = 'join';
			} elsif ($cmd eq 'on') {
				my ($on) = splice @linked_list, $i + 1, 1;
				$from[-1][_table_join_on] = ["on", $on];
			} elsif ($cmd eq 'using') {
				my ($using) = splice @linked_list, $i + 1, 1;
				$from[-1][_table_join_on] = ["using", $using];
			} elsif ($cmd eq 'as') {
				($from[-1][_table_alias]) = splice @linked_list, $i + 1, 1;
			} elsif ($cmd eq 'where') {
				($conditions) = splice @linked_list, $i + 1, 1;
			} elsif ($cmd eq 'group_by') {
				($groupby) = splice @linked_list, $i + 1, 1;
			} elsif ($cmd eq 'having') {
				($having) = splice @linked_list, $i + 1, 1;
			} elsif ($cmd eq 'limit') {
				($limit) = splice @linked_list, $i + 1, 1;
				$limit += 0;
			} elsif ($cmd eq 'offset') {
				($offset) = splice @linked_list, $i + 1, 1;
				$offset += 0;
			} elsif ($cmd eq 'columns') {
				my ($cols) = splice @linked_list, $i + 1, 1;
				if (ref ($cols)) {
					push @columns, @$cols;
				} else {
					push @columns, $cols;
				}
			}
		}
	}
	error_message {
		result  => 'SQLERR',
		message => "No table to build query on"
	} if !@from;
	for (my $idx = 1 ; $idx < @from ; ++$idx) {
		next if $from[$idx][_table_join_on] or not $from[$idx - 1][_table_join];
		next if substr ($from[$idx][_table_name], 0, 1) eq "(";
		my $cta  = $from[$idx][_table_alias];
		my $cto  = make_name($from[$idx][_table_name]);
		my $ucct = ucfirst $from[$idx][_table_name];
		my @join;
		for (my $i = $idx - 1 ; $i >= 0 ; --$i) {
			next if not $from[$i][_table_join];
			my $ptn = $from[$i][_table_name];
			next if substr ($ptn, 0, 1) eq "(";
			my $ucfptn = ucfirst $ptn;
			if ($cto->can("foreignKey$ucfptn")) {
				my $fkfn = "foreignKey$ucfptn";
				my ($ctf, $ptk) = $cto->$fkfn;
				push @join, "$cta.$ctf = " . $from[$i][_table_alias] . ".$ptk";
			} else {
				my $ptno = make_name($ptn);
				if ($ptno->can("foreignKey$ucct")) {
					my $fkfn = "foreignKey$ucct";
					my ($ptf, $ctk) = $ptno->$fkfn;
					push @join, "$cta.$ctk = " . $from[$i][_table_alias] . ".$ptf";
				}
			}
		}
		$from[$idx][_table_join_on] = ["on", join (" and ", @join)];
	}
	my $from = '';
	@columns = ('*') if not @columns;
	my $joined = 0;
	for (my $idx = 0 ; $idx < @from ; ++$idx) {
		if (not $joined) {
			$from .= " " . $from[$idx][_table_name];
			$from .= " " . $from[$idx][_table_alias] if $from[$idx][_table_alias] ne $from[$idx][_table_name];
		}
		if ($from[$idx][_table_join]) {
			my $nt = $from[$idx + 1];
			$from .= " " . $from[$idx][_table_join];
			$from .= " " . $nt->[_table_name];
			$from .= " " . $nt->[_table_alias] if $nt->[_table_alias] ne $nt->[_table_name];
			my $using_on = $nt->[_table_join_on][0];
			if ($using_on eq 'on' and ref $nt->[_table_join_on][1]) {
				my ($on_where, @on_bind) = $sql_abstract->where($nt->[_table_join_on][1]);
				$on_where =~ s/WHERE //;
				push @$query_bind, @on_bind;
				$from .= " $using_on(" . $on_where . ")";
			} else {
				$from .= " $using_on(" . $nt->[_table_join_on][1] . ")";
			}
			$joined = 1;
		} else {
			$from .= "," if $idx != $#from;
			$joined = 0;
		}
	}
	my $ret = "select " . join (", ", @columns) . " from" . $from;
	if (not defiend $where) {
		my $sql_grp     = _parse_groupby($groupby);
		my $having_bind = [];
		if ($sql_grp && defined $having) {
			my $sql_having;
			($sql_having, $having_bind) = _parse_having($having);
			$sql_grp .= " $sql_having";
		}
		if ($conditions) {
			my @where_bind;
			($where, @where_bind) = $sql_abstract->where($conditions);
			push @$query_bind, @where_bind;
		} else {
			$where = '';
		}
		if (defined $sql_grp) {
			$where .= " $sql_grp";
			push @$query_bind, @$having_bind;
		}
		$where .= " limit $limit"   if $limit;
		$where .= " offset $offset" if $offset;
	}
	$ret .= " $where" if $where;
	$ret;
}

sub _parse_groupby {
	my $groupby = $_[0];
	my $sql_grp;
	if (defined $groupby) {
		$sql_grp = "GROUP BY ";
		my @groupby = map { /^\d+$/ ? $_ : qq{"$_"} } (ref ($groupby) ? @$groupby : ($groupby));
		$sql_grp .= join (", ", @groupby);
	}
	$sql_grp;
}

sub _parse_having {
	my $having = $_[0];
	my $sql_having;
	my @having_bind;
	if (defined $having) {
		($sql_having, @having_bind) = $sql_abstract->where($having);
		$sql_having =~ s/\bWHERE\b/HAVING/;
	}
	($sql_having, \@having_bind);
}

sub execute {
	my ($groupby, $having, $up_conditions, $up_order, $up_limit, $up_offset);
	for (my $i = 2 ; $i < @_ ; ++$i) {
		next unless defined $_[$i] and not ref $_[$i];
		if ($_[$i] eq '-group_by') {
			(undef, $groupby) = splice @_, $i, 2;
			--$i;
		} elsif ($_[$i] eq '-having') {
			(undef, $having) = splice @_, $i, 2;
			--$i;
		} elsif ($_[$i] eq '-order_by') {
			(undef, $up_order) = splice @_, $i, 2;
			--$i;
		} elsif ($_[$i] eq '-where') {
			(undef, $up_conditions) = splice @_, $i, 2;
			--$i;
		} elsif ($_[$i] eq '-limit') {
			(undef, $up_limit) = splice @_, $i, 2;
			--$i;
		} elsif ($_[$i] eq '-offset') {
			(undef, $up_offset) = splice @_, $i, 2;
			--$i;
		}
	}
	$tblnum = 1;
	my $sql_grp     = _parse_groupby($groupby);
	my $having_bind = [];
	if ($sql_grp && defined $having) {
		my $sql_having;
		($sql_having, $having_bind) = _parse_having($having);
		$sql_grp .= " $sql_having";
	}
	my ($code, $table, $conditions, $order, $limit, $offset) = @_;
	my $have_conditions = @_ > 2;
	$conditions //= $up_conditions;
	$order      //= $up_order;
	$limit      //= $up_limit;
	$offset     //= $up_offset;
	my $where;
	my $need_where = 0;
	my @where_bind;
	my $simple_table = (not ref $table and index ($table, " ") == -1);
	my $ncn;
	if ($simple_table) {
		$ncn = make_name($table);
		setup_row($table);
		if ($have_conditions and not ref $conditions) {
			my $id = ($ncn->selectKeys())[0]
			  or error_message {
				result  => 'SQLERR',
				message => "unknown primary key",
				query   => "select * from $table",
			  };
			if (defined $conditions) {
				$where      = "where $id = ?";
				@where_bind = ($conditions);
			} else {
				$where = "where $id is null";
			}
		} else {
			$need_where = 1;
		}
	} else {
		$need_where = 1;
	}
	if ($need_where) {
		($where, @where_bind) = $sql_abstract->where($conditions);
	}
	if (defined $sql_grp) {
		$where .= " $sql_grp";
		push @where_bind, @$having_bind;
	}
	if ($order) {
		my ($order_sql, @order_bind) = $sql_abstract->where(undef, $order);
		$where .= " $order_sql";
		push @where_bind, @order_bind;
	}
	if (defined ($limit)) {
		$limit += 0;
		$where .= " limit $limit" if $limit;
	}
	if (defined ($offset)) {
		$offset += 0;
		$where .= " offset $offset" if $offset;
	}
	my $query;
	my @query_bind;
	if ($simple_table) {
		$query = qq{select * from $table $where};
	} else {
		$query = (not ref $table) ? qq{$table $where} : _build_complex_query($table, \@query_bind, $where);
		$ncn = make_name($query);
	}
	'' =~ /()/;
	my $sth;
	return DBIx::Struct::connect->run(
		sub {
			$sth = $_->prepare($query)
			  or error_message {
				result  => 'SQLERR',
				message => $_->errstr,
				query   => $query,
			  };
			$sth->execute(@query_bind, @where_bind)
			  or error_message {
				result     => 'SQLERR',
				message    => $_->errstr,
				query      => $query,
				where_bind => Dumper(\@where_bind),
				query_bind => Dumper(\@query_bind),
				conditions => Dumper($conditions),
			  };
			setup_row($sth, $ncn);
			no strict 'refs';
			if (!"$ncn"->can("dumpSQL")) {
				*{$ncn . "::dumpSQL"} = sub { $query };
				if (@query_bind) {
					*{$ncn . "::needQueryBind"} = sub { 1 };
				} else {
					*{$ncn . "::needQueryBind"} = sub { 0 };
				}
			}
			return $code->($sth, $ncn);
		}
	);
}

sub one_row {
	return execute(
		sub {
			my ($sth, $ncn) = @_;
			my $data = $sth->fetchrow_arrayref;
			$sth->finish;
			return if not $data;
			return $ncn->new($data);
		},
		@_
	);
}

sub all_rows {
	my $mapfunc;
	for (my $i = 0 ; $i < @_ ; ++$i) {
		if (ref ($_[$i]) eq 'CODE') {
			$mapfunc = splice @_, $i, 1;
			last;
		}
	}
	return execute(
		sub {
			my ($sth, $ncn) = @_;
			my @rows;
			my $row;
			if ($mapfunc) {
				while ($row = $sth->fetch) {
					local $_ = $ncn->new([@$row]);
					push @rows, $mapfunc->();
				}
			} else {
				push @rows, $ncn->new([@$row]) while ($row = $sth->fetch);
			}
			return \@rows;
		},
		@_
	);
}

sub new_row {
	my ($table, @data) = @_;
	my $simple_table = (index ($table, " ") == -1);
	error_message {
		result  => 'SQLERR',
		message => "insert row can't work for queries"
	  }
	  unless $simple_table;
	my $ncn = setup_row($table);
	return $ncn->new(@data);
}

1;
