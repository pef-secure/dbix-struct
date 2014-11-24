use ExtUtils::MakeMaker;
WriteMakefile(
	NAME        => "SQL::Struct",
	AUTHOR      => "Pef Secure",
	VERSION     => "0.01",
	INSTALLDIRS => 'site',
	PREREQ_PM   => {
		'DBIx::Connector' => "0.53",
		'Exporter'        => 1,
		'Carp'            => 1,
		'Data::Dumper'    => 1,
		'Digest::MD5'     => 1,
		'warnings'        => 1,
		'strict'          => 1,
		'SQL::Abstract'   => 1,
	},
	dist => {
		DIST_DEFAULT => 'all tardist',
		COMPRESS     => 'gzip -vf',
		SUFFIX       => '.gz',
	},
	clean     => {FILES => '*~',},
	realclean => {FILES => '*~',},
);
