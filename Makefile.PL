use ExtUtils::MakeMaker;
WriteMakefile(
	NAME        => "SQL::Struct",
	AUTHOR      => "Pef Secure",
	VERSION     => "0.03",
	INSTALLDIRS => 'site',
	LICENSE     => "Artistic_2_0",
	META_MERGE  => {
		"meta-spec" => {version => 2},
		resources   => {
			bugtracker => {web => 'https://github.com/pef-secure/dbix-struct/issues'},
			repository => {
				type => 'git',
				url  => 'https://github.com/pef-secure/dbix-struct.git',
				web  => 'https://github.com/pef-secure/dbix-struct',
			},
		},
	},
	PREREQ_PM => {
		'DBIx::Connector' => "0.53",
		'Exporter'        => 0,
		'Carp'            => 0,
		'Data::Dumper'    => 0,
		'Digest::MD5'     => 0,
		'warnings'        => 0,
		'strict'          => 0,
		'SQL::Abstract'   => 0,
	},
	test => {TESTS => "t/*.t"},
	dist => {
		DIST_DEFAULT => 'all tardist',
		COMPRESS     => 'gzip -vf',
		SUFFIX       => '.gz',
	},
	clean     => {FILES => '*~',},
	realclean => {FILES => '*~',},
);
