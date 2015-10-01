use ExtUtils::MakeMaker;
WriteMakefile(
	NAME             => "DBIx::Struct",
	AUTHOR           => "Pef Secure",
	VERSION          => "0.05",
	INSTALLDIRS      => 'site',
	LICENSE          => "artistic_2",
	MIN_PERL_VERSION => "5.14",
	META_MERGE       => {
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
		'DBIx::Connector' => "0.52",
		'Digest::MD5'     => 0,
		'SQL::Abstract'   => 0,
	},
	TEST_REQUIRES => {'Test::More' => 0},
	test          => {TESTS        => "t/*.t"},
	dist          => {
		DIST_DEFAULT => 'all tardist',
		COMPRESS     => 'gzip -vf',
		SUFFIX       => '.gz',
	},
	clean     => {FILES => '*~',},
	realclean => {FILES => '*~',},
);
