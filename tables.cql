CREATE TABLE monkeySpecies (
                               species text PRIMARY KEY,
                               common_name text,
                               population varint,
                               average_size int
) WITH comment='Important biological records'
      AND read_repair_chance = 1.0;

CREATE TABLE timeline (
                          userid uuid,
                          posted_month int,
                          posted_time uuid,
                          body text,
                          posted_by text,
                          PRIMARY KEY (userid, posted_month, posted_time)
) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };

CREATE TABLE loads (
                       machine inet,
                       cpu int,
                       mtime timeuuid,
                       load float,
                       PRIMARY KEY ((machine, cpu), mtime)
    ) WITH CLUSTERING ORDER BY (mtime DESC);

CREATE TABLE users_picture (
                               userid uuid,
                               pictureid uuid,
                               body text,
                               posted_by text,
                               PRIMARY KEY (userid, pictureid, posted_by)
) WITH compression = {'sstable_compression': 'LZ4Compressor'};


CREATE TABLE data_atrest (
                             pk text PRIMARY KEY,
                             c0 int
) WITH scylla_encryption_options = {
      'cipher_algorithm' : 'AES/ECB/PKCS5Padding',
      'secret_key_strength' : 128,
      'key_provider': 'LocalFileSystemKeyProviderFactory',
      'secret_key_file': '/etc/scylla/data_encryption_keys/secret_key'};

CREATE TABLE caching (
                         k int PRIMARY KEY,
                         v1 int,
                         v2 int,
) WITH caching = {'enabled': 'true'};