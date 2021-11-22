# v4.0 Release

## Improvements

* [#6806](https://github.com/FirebirdSQL/firebird/issues/6806): Rename RSA_SIGN and RSA_VERIFY  
  Reference(s): [/doc/sql.extensions/README.builtin_functions.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.builtin_functions.txt), [/doc/sql.extensions/README.keywords](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.keywords)  
  Contributor(s): Alex Peshkoff

* [#6786](https://github.com/FirebirdSQL/firebird/issues/6786): Add session time zone to system context  
  Reference(s): [/doc/sql.extensions/README.time_zone.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.time_zone.md), [/doc/sql.extensions/README.context_variables2](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.context_variables2)  
  Contributor(s): Adriano dos Santos Fernandes

* [#6769](https://github.com/FirebirdSQL/firebird/issues/6769): More efficient implementation of SUBSTRING for UTF8 character set  
  Contributor(s): Adriano dos Santos Fernandes

* [#6767](https://github.com/FirebirdSQL/firebird/issues/6767): Add ability for a replication plugin to refuse attachment  
  Contributor(s): Dmitry Yemanov

* [#6748](https://github.com/FirebirdSQL/firebird/issues/6748): Freeing a statement using DSQL_drop or DSQL_unprepare should send free packet immediately  
  Contributor(s): Vlad Khorsun

* [#6715](https://github.com/FirebirdSQL/firebird/issues/6715): Provide time zone ID constants in public headers  
  Contributor(s): Adriano dos Santos Fernandes

* [#6253](https://github.com/FirebirdSQL/firebird/issues/6253): RDB$GET_TRANSACTION_CN should work the same way in Super and Classic  
  Contributor(s): Vlad Khorsun

## Bugfixes

* [#6816](https://github.com/FirebirdSQL/firebird/issues/6816): Illegal output length in base64/hex encode/decode functions  
  Contributor(s): Alex Peshkoff

* [#6812](https://github.com/FirebirdSQL/firebird/issues/6812): BASE64_ENCODE and HEX_ENCODE can exceed maximum widths for VARCHAR  
  Contributor(s): Alex Peshkoff

* [#6808](https://github.com/FirebirdSQL/firebird/issues/6808): Segfault in encrypt/decrypt functions when their first argument is NULL  
  Contributor(s): Alex Peshkoff

* [#6805](https://github.com/FirebirdSQL/firebird/issues/6805): RDB$TYPES has incorrect entries for RDB$TYPE 28 and 29 in RDB$TYPE_NAME column  
  Contributor(s): Alex Peshkoff

* [#6804](https://github.com/FirebirdSQL/firebird/issues/6804): Assertion in tomcrypt when key length for RC4 is too small  
  Contributor(s): Alex Peshkoff

* [#6802](https://github.com/FirebirdSQL/firebird/issues/6802): When the statement timeout is set, it causes the lock manager to delay reporting deadlocks until timeout is expired  
  Contributor(s): Vlad Khorsun

* [#6800](https://github.com/FirebirdSQL/firebird/issues/6800): Client config DefaultTimeZone is not passed to server when isc_dpb_session_time_zone is not set  
  Contributor(s): Adriano dos Santos Fernandes

* [#6797](https://github.com/FirebirdSQL/firebird/issues/6797): Functions DECRYPT and RSA_DECRYPT return VARCHAR CHARACTER SET NONE instead of VARBINARY (VARCHAR CHARACTER SET OCTETS)  
  Contributor(s): Alex Peshkoff

* [#6796](https://github.com/FirebirdSQL/firebird/issues/6796): Buffer overflow when padding line with national characters causes ISQL to crash  
  Contributor(s): Adriano dos Santos Fernandes

* [#6795](https://github.com/FirebirdSQL/firebird/issues/6795): Replication gets stuck due to "Blob xxx.xx is not found for table xxx" error  
  Contributor(s): Dmitry Yemanov

* [#6790](https://github.com/FirebirdSQL/firebird/issues/6790): MON$ATTACHMENTS.MON$TIMESTAMP is incorrect when DefaultTimeZone is configured with time zone different from the server's default  
  Contributor(s): Adriano dos Santos Fernandes

* [#6787](https://github.com/FirebirdSQL/firebird/issues/6787): MON$ATTACHMENTS.MON$TIMESTAMP should use its session original time zone  
  Contributor(s): Adriano dos Santos Fernandes

* [#6785](https://github.com/FirebirdSQL/firebird/issues/6785): Problem when restoring the database on Firebird 4 RC1  
  Contributor(s): Vlad Khorsun

* [#6782](https://github.com/FirebirdSQL/firebird/issues/6782): Cannot get "records fetched" for selectable procedures in trace  
  Contributor(s): Vlad Khorsun

* [#6781](https://github.com/FirebirdSQL/firebird/issues/6781): Crashing (due to UDF exception) process hangs  
  Contributor(s): Vlad Khorsun

* [#6780](https://github.com/FirebirdSQL/firebird/issues/6780): firebird.msg is missing in Firebird Android builds  
  Contributor(s): Alex Peshkoff

* [#6778](https://github.com/FirebirdSQL/firebird/issues/6778): Inconsistent cursor-driven deletion  
  Contributor(s): Dmitry Yemanov

* [#6777](https://github.com/FirebirdSQL/firebird/issues/6777): AV when the engine shuts down and cancels an attachment waiting in the lock manager  
  Contributor(s): Vlad Khorsun

* [#6768](https://github.com/FirebirdSQL/firebird/issues/6768): Restore backup on RawDevice (error during "open O_CREAT" operation for file "/dev/sda1")  
  Contributor(s): Alex Peshkoff

* [#6766](https://github.com/FirebirdSQL/firebird/issues/6766): Replication plugin can crash engine returning nullptr from startTransaction() method  
  Contributor(s): Dmitry Yemanov

* [#6761](https://github.com/FirebirdSQL/firebird/issues/6761): Hash join cannot match records using some TIME ZONE / DECFLOAT keys  
  Contributor(s): Dmitry Yemanov

* [#6759](https://github.com/FirebirdSQL/firebird/issues/6759): Results of concatenation with blob has no info about collation of source columns (which are declared with such info)  
  Contributor(s): Vlad Khorsun

* [#6758](https://github.com/FirebirdSQL/firebird/issues/6758): COMPUTED BY column looses charset and collate of source field <F> when <F> is either of type BLOB or VARCHAR casted to BLOB  
  Contributor(s): Vlad Khorsun

* [#6756](https://github.com/FirebirdSQL/firebird/issues/6756): Error "no current record for fetch operation" when sorting by a international string  
  Contributor(s): Dmitry Yemanov

* [#6754](https://github.com/FirebirdSQL/firebird/issues/6754): Connect to database that contains broken pages can lead to FB crash  
  Contributor(s): Alex Peshkoff

* [#6753](https://github.com/FirebirdSQL/firebird/issues/6753): AV in the engine when StatementTimeout is active for user statement and some internal DSQL statement was executed as part of overall execution process  
  Contributor(s): Vlad Khorsun

* [#6752](https://github.com/FirebirdSQL/firebird/issues/6752): Segfaults in fbclient when receiving invalid / unexpected data from server  
  Contributor(s): Alex Peshkoff

* [#6751](https://github.com/FirebirdSQL/firebird/issues/6751): Various segfaults in fbclient  
  Contributor(s): Alex Peshkoff

* [#6750](https://github.com/FirebirdSQL/firebird/issues/6750): CAST of Infinity values to FLOAT doesn't work  
  Contributor(s): Alex Peshkoff

* [#6749](https://github.com/FirebirdSQL/firebird/issues/6749): Error "Invalid time zone (+08). Falling back to displacement" in firebird.log  
  Contributor(s): Adriano dos Santos Fernandes

* [#6747](https://github.com/FirebirdSQL/firebird/issues/6747): Wrong message when connecting to tiny trash database file  
  Contributor(s): Alex Peshkoff

* [#6746](https://github.com/FirebirdSQL/firebird/issues/6746): Regression: CREATE DATABASE fails with 'Token unknown' error when DB name is enclosed in double quotes and 'DEFAULT CHARACTER SET' is specified after DB name  
  Contributor(s): Adriano dos Santos Fernandes

* [#6745](https://github.com/FirebirdSQL/firebird/issues/6745): Protect included tomcrypt library from being overwritten by a system package  
  Contributor(s): Alex Peshkoff

* [#6738](https://github.com/FirebirdSQL/firebird/issues/6738): Segfault when GFIX requests for database page buffer more memory than available from OS  
  Contributor(s): Alex Peshkoff

* [#6734](https://github.com/FirebirdSQL/firebird/issues/6734): Provide same results for date arithmetics when date is changed by values near +/-max(bigint)  
  Contributor(s): Alex Peshkoff

* [#6733](https://github.com/FirebirdSQL/firebird/issues/6733): Attempt to create database with page_size >= 65536 makes DB with actual page size = 4KB or 8KB, but not 32KB as it should  
  Contributor(s): Alex Peshkoff

* [#6731](https://github.com/FirebirdSQL/firebird/issues/6731): Segfault when shutting down database which got encrypted by another process  
  Contributor(s): Alex Peshkoff

* [#6729](https://github.com/FirebirdSQL/firebird/issues/6729): Regression: GSTAT with switch -t executed via services fails with "found unknown switch" error  
  Contributor(s): Alex Peshkoff

* [#6727](https://github.com/FirebirdSQL/firebird/issues/6727): Synchronous replication to localhost hangs on disconnect  
  Contributor(s): Dmitry Yemanov

* [#6724](https://github.com/FirebirdSQL/firebird/issues/6724): Inconsistent translation "string->timestamp->string->timestamp" in Dialect 1  
  Contributor(s): Adriano dos Santos Fernandes

* [#6719](https://github.com/FirebirdSQL/firebird/issues/6719): User without ALTER ANY ROLE privilege can use COMMENT ON ROLE  
  Contributor(s): Alex Peshkoff

* [#6718](https://github.com/FirebirdSQL/firebird/issues/6718): Event delivery could be missed when local (XNET) protocol is used  
  Contributor(s): Vlad Khorsun

* [#6717](https://github.com/FirebirdSQL/firebird/issues/6717): FETCH ABSOLUTE and RELATIVE beyond bounds of cursor should always position immediately before-first or after-last  
  Contributor(s): Dmitry Yemanov

* [#6716](https://github.com/FirebirdSQL/firebird/issues/6716): FETCH RELATIVE has an off by one error for the first row  
  Contributor(s): Dmitry Yemanov

* [#6710](https://github.com/FirebirdSQL/firebird/issues/6710): COMMENT ON USER can only apply comment on user defined by the default user manager plugin  
  Contributor(s): Alex Peshkoff

* [#6709](https://github.com/FirebirdSQL/firebird/issues/6709): GBAK discards replica mode during backup/restore  
  Contributor(s): Dmitry Yemanov

* [#6708](https://github.com/FirebirdSQL/firebird/issues/6708): Rare race condition in Plugin Manager could lead to the server crash  
  Contributor(s): Vlad Khorsun

* [#6700](https://github.com/FirebirdSQL/firebird/issues/6700): Wire compression causes sporadic "Error reading data from the connection" errors  
  Contributor(s): Alex Peshkoff

* [#6698](https://github.com/FirebirdSQL/firebird/issues/6698): Comments before the first line of code are removed  
  Contributor(s): Adriano dos Santos Fernandes

* [#6679](https://github.com/FirebirdSQL/firebird/issues/6679): CLOOP envelopes are wrong regarding IStatus  
  Contributor(s): Alex Peshkoff

* [#6265](https://github.com/FirebirdSQL/firebird/issues/6265): Segfault when using expression index with complex expression  
  Contributor(s): Vlad Khorsun

* [#5784](https://github.com/FirebirdSQL/firebird/issues/5784): When 32-bit and 64-bit Firebird 3 servers run on a single Windows machine concurrently, Firebird services freeze several minutes after first disconnect  
  Contributor(s): Vlad Khorsun

* [#3810](https://github.com/FirebirdSQL/firebird/issues/3810): Wrong or missing IS NULL optimization (regression)  
  Contributor(s): Vlad Khorsun

* [#3106](https://github.com/FirebirdSQL/firebird/issues/3106): Many indexed reads in a compound index with NULLs  
  Contributor(s): Vlad Khorsun

* [#2469](https://github.com/FirebirdSQL/firebird/issues/2469): Stored procedure recursively called by calculated field fails after reconnect  
  Contributor(s): Vlad Khorsun

# v4.0 Release Candidate 1 (01-Feb-2021)

## New features

* [CORE-5832](http://tracker.firebirdsql.org/browse/CORE-5832): Add command to reset user session environment to its initial (default) state  
  Contributor(s): Vlad Khorsun

* [CORE-3708](http://tracker.firebirdsql.org/browse/CORE-3708): Add virtual table RDB$CONFIG that shows actual configuration settings  
  Contributor(s): Vlad Khorsun

## Improvements

* [CORE-6474](http://tracker.firebirdsql.org/browse/CORE-6474): Report replica mode through isc_database_info, MON$DATABASE and SYSTEM context  
  Contributor(s): Dmitry Yemanov

* [CORE-6469](http://tracker.firebirdsql.org/browse/CORE-6469): Ability to trace session management commands (e.g. ALTER SESSION RESET)  
  Contributor(s): Vlad Khorsun

* [CORE-6429](http://tracker.firebirdsql.org/browse/CORE-6429): Timezone offset in timestamp/time literal, CAST, SET TIME ZONE and AT TIME ZONE should follow SQL standard syntax only  
  Reference(s): [/doc/sql.extensions/README.time_zone.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.time_zone.md)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6413](http://tracker.firebirdsql.org/browse/CORE-6413): Remove PIDFile/-pidfile directive/option from firebird Super(Server/Classic) systemd unit  
  Contributor(s): Alex Peshkoff

* [CORE-6395](http://tracker.firebirdsql.org/browse/CORE-6395): Allow usage of time zone displacement in config DefaultTimeZone  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6366](http://tracker.firebirdsql.org/browse/CORE-6366): Check presence of tomcrypt & curses libraries before installing Firebird  
  Contributor(s): Alex Peshkoff

* [CORE-6342](http://tracker.firebirdsql.org/browse/CORE-6342): INT128 as explicit basic type for high precision numerics  
  Reference(s): [/doc/sql.extensions/README.data_types](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.data_types)  
  Contributor(s): Alex Peshkoff

* [CORE-6320](http://tracker.firebirdsql.org/browse/CORE-6320): Replace Util methods to get interface pointer by legacy handle with plain functions  
  Contributor(s): Alex Peshkoff

* [CORE-6308](http://tracker.firebirdsql.org/browse/CORE-6308): Make it possible to update list of time zones (names and ids) without source code recompilation  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6300](http://tracker.firebirdsql.org/browse/CORE-6300): Ability to retrieve next attachment id and next statement id  
  Reference(s): [/doc/README.monitoring_tables](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.monitoring_tables)  
  Contributor(s): Dmitry Yemanov

* [CORE-5085](http://tracker.firebirdsql.org/browse/CORE-5085): Allow to fixup (nbackup) database via Services API  
  Reference(s): [/doc/README.services_extension](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.services_extension)  
  Contributor(s): Alex Peshkoff

* [CORE-4841](http://tracker.firebirdsql.org/browse/CORE-4841): Report explicit message about missing password in CREATE (OR ALTER) USER  
  Contributor(s): Alex Peshkoff

* [CORE-2650](http://tracker.firebirdsql.org/browse/CORE-2650): Improve sorting performance when long VARCHARs are involved  
  Contributor(s): Dmitry Yemanov

## Bugfixes

* [CORE-6475](http://tracker.firebirdsql.org/browse/CORE-6475): Memory leak when running EXECUTE STATEMENT with named parameters  
  Contributor(s): Vlad Khorsun

* [CORE-6472](http://tracker.firebirdsql.org/browse/CORE-6472): Wrong byte order for UUIDs in gstat and monitoring output  
  Contributor(s): Dmitriy Sibiryakov

* [CORE-6460](http://tracker.firebirdsql.org/browse/CORE-6460): Incorrect query result when using named window  
  Contributor(s): Vlad Khorsun

* [CORE-6453](http://tracker.firebirdsql.org/browse/CORE-6453): EXECUTE STATEMENT fails on FB 4.x if containing time/timestamp with time zone parameters  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6450](http://tracker.firebirdsql.org/browse/CORE-6450): Races in the security databases cache could lead to the server crash  
  Contributor(s): Alex Peshkoff

* [CORE-6447](http://tracker.firebirdsql.org/browse/CORE-6447): Unexpectedly different text of message for parameterized expression starting from second run  
  Contributor(s): Vlad Khorsun

* [CORE-6441](http://tracker.firebirdsql.org/browse/CORE-6441): Srp plugin keeps connection after database has been removed for ~10 seconds  
  Contributor(s): Alex Peshkoff

* [CORE-6440](http://tracker.firebirdsql.org/browse/CORE-6440): Expression indexes containing COALESCE inside cannot be matched by the optimizer after migration from v2.5 to v3.0  
  Contributor(s): Dmitry Yemanov

* [CORE-6438](http://tracker.firebirdsql.org/browse/CORE-6438): ISQL: bad headers when text columns has >= 80 characters  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6437](http://tracker.firebirdsql.org/browse/CORE-6437): GFIX cannot set big value for page buffers  
  Contributor(s): Vlad Khorsun

* [CORE-6433](http://tracker.firebirdsql.org/browse/CORE-6433): Server could crash during a daily maintenance / set statistics index  
  Contributor(s): Alex Peshkoff

* [CORE-6432](http://tracker.firebirdsql.org/browse/CORE-6432): Possible buffer overflow in client library in Attachment::getInfo() call  
  Contributor(s): Alex Peshkoff

* [CORE-6427](http://tracker.firebirdsql.org/browse/CORE-6427): Whitespace as date separator causes conversion error  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6426](http://tracker.firebirdsql.org/browse/CORE-6426): Assertion when the batch is executed without a BLOB field  
  Contributor(s): Alex Peshkoff

* [CORE-6425](http://tracker.firebirdsql.org/browse/CORE-6425): Exception in client library in IAttachment::createBatch()  
  Contributor(s): Alex Peshkoff

* [CORE-6421](http://tracker.firebirdsql.org/browse/CORE-6421): Parameter in offset expression in LAG, LEAD, NTH_VALUE window functions requires explicit cast to BIGINT or INTEGER  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6419](http://tracker.firebirdsql.org/browse/CORE-6419): Truncation of strings to put in MON$ tables do not work correctly  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6415](http://tracker.firebirdsql.org/browse/CORE-6415): Error "malformed string' is raised instead of "expected: N, actual: M" when UTF-8 charset is used and default value is longer than the column length  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6414](http://tracker.firebirdsql.org/browse/CORE-6414): Error "expected length N, actual M" contains wrong value of M when UTF-8 charset is used in the field declaration  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6412](http://tracker.firebirdsql.org/browse/CORE-6412): Firebird was freezing when trying to manage users via triggers  
  Contributor(s): Alex Peshkoff

* [CORE-6408](http://tracker.firebirdsql.org/browse/CORE-6408): RETURNING clause in the MERGE statement cannot reference column in aliased target table using qualified reference (alias.column) if DELETE action present  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6403](http://tracker.firebirdsql.org/browse/CORE-6403): Some PSQL statements may lead to exceptions report wrong line/column  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6398](http://tracker.firebirdsql.org/browse/CORE-6398): Error converting string with hex representation of INTEGER to SMALLINT  
  Contributor(s): Alex Peshkoff

* [CORE-6397](http://tracker.firebirdsql.org/browse/CORE-6397):  Message length error with COALESCE and TIME/TIMESTAMP WITHOUT TIME ZONE and WITH TIME ZONE  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6389](http://tracker.firebirdsql.org/browse/CORE-6389): Using binary string literal to assign to user-defined blob sub-types yield conversion error  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6387](http://tracker.firebirdsql.org/browse/CORE-6387): Client process was aborting due to bugs inside the ChaCha plugin
  Contributor(s): Alex Peshkoff

* [CORE-6386](http://tracker.firebirdsql.org/browse/CORE-6386): ALTER SEQUENCE RESTART WITH <n> should not change the initial sequence START value  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6385](http://tracker.firebirdsql.org/browse/CORE-6385): Wrong line and column information after IF statement  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6379](http://tracker.firebirdsql.org/browse/CORE-6379): Bugcheck 179 (decompression overran buffer)  
  Contributor(s): Vlad Khorsun

* [CORE-6377](http://tracker.firebirdsql.org/browse/CORE-6377): Unable to restore database with tables using GENERATED ALWAYS AS IDENTITY columns  
  Contributor(s): Alex Peshkoff

* [CORE-6376](http://tracker.firebirdsql.org/browse/CORE-6376): IDENTITY column with explicit START WITH or INCREMENT BY starts with wrong value  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6357](http://tracker.firebirdsql.org/browse/CORE-6357): LEAD() and LAG() do not allow to specify 3rd argument of INT128 datatype  
  Contributor(s): Alex Peshkoff

* [CORE-6356](http://tracker.firebirdsql.org/browse/CORE-6356): ROUND() does not allow second argument >=1 when its first argument is more than MAX_BIGINT / 10  
  Contributor(s): Alex Peshkoff

* [CORE-6355](http://tracker.firebirdsql.org/browse/CORE-6355): TRUNC() does not accept second argument = -128 (but shows it as required boundary in error message)  
  Contributor(s): Alex Peshkoff

* [CORE-6353](http://tracker.firebirdsql.org/browse/CORE-6353): INT128 data type has problems with some PSQL objects  
  Contributor(s): Alex Peshkoff

* [CORE-6344](http://tracker.firebirdsql.org/browse/CORE-6344): Invalid return type for functions with INT128 / Numeric(38) argument  
  Contributor(s): Alex Peshkoff

* [CORE-6337](http://tracker.firebirdsql.org/browse/CORE-6337): Sub-type information is lost when calculating arithmetic expressions  
  Contributor(s): Alex Peshkoff

* [CORE-6336](http://tracker.firebirdsql.org/browse/CORE-6336): Error "Implementation of text subtype <NNNN> not located" on attempt to use some collations defined in fbintl.conf  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6335](http://tracker.firebirdsql.org/browse/CORE-6335): INSERT ... RETURNING does not require a SELECT privilege  
  Contributor(s): Dmitry Yemanov

* [CORE-6328](http://tracker.firebirdsql.org/browse/CORE-6328): FB4 Beta 2 may still be using the current date for TIME WITH TIME ZONE and extended wire protocol  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6325](http://tracker.firebirdsql.org/browse/CORE-6325): NTILE/RANK/PERCENT_RANK may cause problems in big/complex statements  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6318](http://tracker.firebirdsql.org/browse/CORE-6318): CAST('NOW' as TIME) raises a conversion error  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6316](http://tracker.firebirdsql.org/browse/CORE-6316): Unable to specify new 32KB page size in CREATE DATABASE statement  
  Contributor(s): Alex Peshkoff

* [CORE-6305](http://tracker.firebirdsql.org/browse/CORE-6305): Android port build failure  
  Contributor(s): Alex Peshkoff

* [CORE-6303](http://tracker.firebirdsql.org/browse/CORE-6303): Error writing to TIMESTAMP/TIME WITH TIME ZONE array  
  Contributor(s): Alex Peshkoff

* [CORE-6302](http://tracker.firebirdsql.org/browse/CORE-6302): Error writing an array of NUMERIC(24,6) to the database  
  Contributor(s): Alex Peshkoff

* [CORE-6084](http://tracker.firebirdsql.org/browse/CORE-6084): CREATE SEQUENCE START WITH has wrong initial value  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6023](http://tracker.firebirdsql.org/browse/CORE-6023): FB4 is unable to overwrite older ODS database  
  Contributor(s): Alex Peshkoff

* [CORE-5838](http://tracker.firebirdsql.org/browse/CORE-5838): Rotated trace files are locked by the engine  
  Contributor(s): Vlad Khorsun

* [CORE-4985](http://tracker.firebirdsql.org/browse/CORE-4985): A non-privileged user could implicitly count records in a restricted table  
  Contributor(s): Dmitry Yemanov

* [CORE-2274](http://tracker.firebirdsql.org/browse/CORE-2274): MERGE has a non-standard behaviour, accepts multiple matches  
  Contributor(s): Vlad Khorsun


# v4.0 Beta 2 (14-May-2020)

## New features

* [CORE-6287](http://tracker.firebirdsql.org/browse/CORE-6287): SET BIND OF type1 TO type2  
  Reference(s): [/doc/sql.extensions/README.set_bind.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.set_bind.md)  
  Contributor(s): Alex Peshkoff

* [CORE-6285](http://tracker.firebirdsql.org/browse/CORE-6285): SQL-level replication management  
  Reference(s): [/doc/README.replication.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.replication.md), [/doc/sql.extensions/README.ddl.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.ddl.txt)  
  Contributor(s): Dmitry Yemanov

* [CORE-6109](http://tracker.firebirdsql.org/browse/CORE-6109): Changed FLOAT to a SQL standard compliant FLOAT datatype  
  Reference(s): [/doc/sql.extensions/README.floating_point_types.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.floating_point_types.md)  
  Contributor(s): Mark Rotteveel

* [CORE-6018](http://tracker.firebirdsql.org/browse/CORE-6018): Make it possible to start multiple transactions (possibly in different attachments) using the same initial transaction snapshot  
  Reference(s): [/doc/README.transaction_at_snapshot.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.transaction_at_snapshot.md)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-4933](http://tracker.firebirdsql.org/browse/CORE-4933): Add better transaction control to ISQL  
  Reference(s): [/doc/README.isql_enhancements.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.isql_enhancements.txt)  
  Contributor(s): Vlad Khorsun

* [CORE-3435](http://tracker.firebirdsql.org/browse/CORE-3435): Lateral derived tables  
  Reference(s): [/doc/sql.extensions/README.derived_tables.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.derived_tables.txt)  
  Contributor(s): Dmitry Yemanov

## Improvements

* [CORE-6286](http://tracker.firebirdsql.org/browse/CORE-6286): Make usage of TIMESTAMP/TIME WITH TIME ZONE convenient for users when appropriate ICU library is not installed on the client side  
  Reference(s): [/doc/sql.extensions/README.time_zone.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.time_zone.md), [/doc/sql.extensions/README.set_bind.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.set_bind.md)   
  Contributor(s): Alex Peshkoff

* [CORE-6279](http://tracker.firebirdsql.org/browse/CORE-6279): Put options in user management statements in any order  
  Reference(s): [/doc/sql.extensions/README.user_management](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.user_management)  
  Contributor(s): Alex Peshkoff

* [CORE-6278](http://tracker.firebirdsql.org/browse/CORE-6278): Efficient table scans for DBKEY-based range conditions  
  Contributor(s): Dmitry Yemanov

* [CORE-6274](http://tracker.firebirdsql.org/browse/CORE-6274): Increase parse speed of long queries  
  Note(s): Backported into v3.0.6  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6268](http://tracker.firebirdsql.org/browse/CORE-6268): Add methods to set various names (field, relation, etc.) in metadata builder  
  Reference(s): [/doc/Using_OO_API.html](https://github.com/FirebirdSQL/firebird/raw/master/doc/Using_OO_API.html)  
  Contributor(s): Alex Peshkoff

* [CORE-6239](http://tracker.firebirdsql.org/browse/CORE-6239): Procedures and EXECUTE BLOCK without RETURNS should not be allowed to use SUSPEND  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6237](http://tracker.firebirdsql.org/browse/CORE-6237): Performance problem when using SRP plugin  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6220](http://tracker.firebirdsql.org/browse/CORE-6220): Enable delivery of known to the client key to any connection to the server  
  Contributor(s): Alex Peshkoff

* [CORE-6219](http://tracker.firebirdsql.org/browse/CORE-6219): Add support for special (inf/nan) values when sorting DECFLOAT values  
  Contributor(s): Alex Peshkoff

* [CORE-6095](http://tracker.firebirdsql.org/browse/CORE-6095): Extend trace record for COMMIT/ROLLBACK RETAINING to allow chaining of transaction ids  
  Note(s): Backported into v3.0.6  
  Contributor(s): Vlad Khorsun

* [CORE-6069](http://tracker.firebirdsql.org/browse/CORE-6069): Show OS-specific error when entrypont is not found in dynamic library  
  Contributor(s): Alex Peshkoff

* [CORE-6058](http://tracker.firebirdsql.org/browse/CORE-6058): Change behavior of skipped and repeated wall times within time zones  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6049](http://tracker.firebirdsql.org/browse/CORE-6049): Builtin functions converting binary string to hexadecimal representation and vice versa  
  Reference(s): [/doc/sql.extensions/README.builtin_functions.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.builtin_functions.txt)  
  Contributor(s): Alex Peshkoff

* [CORE-6048](http://tracker.firebirdsql.org/browse/CORE-6048): Provide ability to see current state of DB encryption  
  Reference(s): [/doc/README.monitoring_tables](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.monitoring_tables)  
  Contributor(s): Alex Peshkoff

* [CORE-6032](http://tracker.firebirdsql.org/browse/CORE-6032): Add DPB properties for time zone bind and decfloat configuration  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6017](http://tracker.firebirdsql.org/browse/CORE-6017): Add transaction info fb\_info\_tra\_snapshot\_number  
  Contributor(s): Vlad Khorsun

* [CORE-5658](http://tracker.firebirdsql.org/browse/CORE-5658): Execute statement with excess parameters  
  Reference(s): [/doc/sql.extensions/README.execute_statement2](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.execute_statement2)  
  Contributor(s): Vlad Khorsun

* [CORE-5538](http://tracker.firebirdsql.org/browse/CORE-5538): Add ability to backup/restore only those (several) tables which are enumerated as command line argument (pattern)  
  Reference(s): [/doc/README.gbak](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.gbak)  
  Contributor(s): Dimitry Sibiryakov

* [CORE-4726](http://tracker.firebirdsql.org/browse/CORE-4726): Implement REcreate user <user_name> password <user_pwd>  
  Contributor(s): Alex Peshkoff

* [CORE-4579](http://tracker.firebirdsql.org/browse/CORE-4579): Distribute separate header files instead merged one  
  Contributor(s): Dimitry Sibiryakov

* [CORE-3983](http://tracker.firebirdsql.org/browse/CORE-3983): Ability to authenticate user in "EXECUTE STATEMENT ON EXTERNAL DATA SOURCE" by hash of the current password  
  Contributor(s): Alex Peshkoff

## Bugfixes

* [CORE-6290](http://tracker.firebirdsql.org/browse/CORE-6290): Hex number used at the end of statement may read invalid memory and produce wrong values or exceptions  
  Note(s): Backported into v3.0.6  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6283](http://tracker.firebirdsql.org/browse/CORE-6283): isNullable() in message metadata, returned by metadata builder, does not match with datatype set by setType() in metadata builder  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6282](http://tracker.firebirdsql.org/browse/CORE-6282): Change type of MON$ATTACHMENTS.MON$IDLE\_TIMER and MON$STATEMENTS.MON$STATEMENT\_TIMER to TIMESTAMP WITH TIME ZONE  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6281](http://tracker.firebirdsql.org/browse/CORE-6281): Invalid timestamp errors with RDB$TIME\_ZONE_UTIL.TRANSITIONS  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6280](http://tracker.firebirdsql.org/browse/CORE-6280): MERGE statement loses parameters in WHEN (NOT) MATCHED clause that will never be matched, crashes server in some situations  
  Note(s): Backported into v3.0.6  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6272](http://tracker.firebirdsql.org/browse/CORE-6272): Failed attach to a database is not traced  
  Contributor(s): Alex Peshkoff

* [CORE-6266](http://tracker.firebirdsql.org/browse/CORE-6266): Deleting records from MON$ATTACHMENTS using ORDER BY clause doesn't close the corresponding attachments  
  Note(s): Backported into v3.0.6  
  Contributor(s): Dmitry Yemanov

* [CORE-6265](http://tracker.firebirdsql.org/browse/CORE-6265): Mapping rules are destroyed by backup / restore  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6264](http://tracker.firebirdsql.org/browse/CORE-6264): GBAK with PIPE to stdout: invalid content if user '-se <host>:service_mgr' command switch  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6262](http://tracker.firebirdsql.org/browse/CORE-6262): SHOW DOMAIN/TABLE does not display character set of system objects  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6260](http://tracker.firebirdsql.org/browse/CORE-6260): Warnings are not always displayed in ISQL  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6254](http://tracker.firebirdsql.org/browse/CORE-6254): Server crashes when using SET TRANSACTION and ON TRANSACTION START trigger uses EXECUTE STATEMENT against current transaction  
  Note(s): Backported into v3.0.6  
  Contributor(s): Vlad Khorsun

* [CORE-6253](http://tracker.firebirdsql.org/browse/CORE-6253): Crash caused by the locked fb_lock file  
  Note(s): Backported into v3.0.6  
  Contributor(s): Vlad Khorsun

* [CORE-6252](http://tracker.firebirdsql.org/browse/CORE-6252): UNIQUE CONSTRAINT violation  
  Note(s): Backported into v3.0.6  
  Contributor(s): Vlad Khorsun

* [CORE-6251](http://tracker.firebirdsql.org/browse/CORE-6251): Crash when built-in function LEFT or RIGHT is missing its 2nd argument  
  Note(s): Backported into v3.0.6  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6250](http://tracker.firebirdsql.org/browse/CORE-6250): Signature mismatch when creating package body on identical packaged procedure header  
  Note(s): Backported into v3.0.6  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6248](http://tracker.firebirdsql.org/browse/CORE-6248): A number of errors when database name is longer than 255 symbols  
  Contributor(s): Alex Peshkoff

* [CORE-6243](http://tracker.firebirdsql.org/browse/CORE-6243): Regression: v4 Beta 1 rejects POSITION element of v2.5 defined SQL2003 CREATE TRIGGER syntax  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6241](http://tracker.firebirdsql.org/browse/CORE-6241): Values greater than number of days between 01.01.0001 and 31.12.9999 (=3652058) can be added or subtracted from DATE  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6238](http://tracker.firebirdsql.org/browse/CORE-6238): DECFLOAT: subtraction Num1 - Num2 leads to "Decimal float overflow" if Num2 is specified in scientific notation and less than max double (1.7976931348623157e308)  
  Contributor(s): Alex Peshkoff

* [CORE-6236](http://tracker.firebirdsql.org/browse/CORE-6236): RDB$TIME_ZONE_UTIL package has wrong privilege for PUBLIC  
  Contributor(s): Adriano dos Santos Fernandes, Dmitry Yemanov

* [CORE-6233](http://tracker.firebirdsql.org/browse/CORE-6233): Wrong dependencies of stored function on view after backup/restore  
  Note(s): Backported into v3.0.6  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6231](http://tracker.firebirdsql.org/browse/CORE-6231): Server crashes on shutdown of XNET connection to a local database when events have been registered  
  Note(s): Backported into v3.0.6  
  Contributor(s): Vlad Khorsun

* [CORE-6230](http://tracker.firebirdsql.org/browse/CORE-6230): Unable to connect with database if security.db reference is removed from databases.conf file  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6227](http://tracker.firebirdsql.org/browse/CORE-6227): isc\_info\_svc\_user\_dbpath always returns alias of main security database  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6224](http://tracker.firebirdsql.org/browse/CORE-6224): Server crash: re-destruction of the rem_port object  
  Note(s): Backported into v3.0.6  
  Contributor(s): Dmitry Kovalenko, Alex Peshkoff

* [CORE-6221](http://tracker.firebirdsql.org/browse/CORE-6221): Incorrect (throw-based) allocFunc for zlib1: memory leak is possible  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6218](http://tracker.firebirdsql.org/browse/CORE-6218): COUNT(DISTINCT <DECFLOAT_FIELD>) leads FB to crash when there are duplicate values of this field  
  Contributor(s): Alex Peshkoff

* [CORE-6217](http://tracker.firebirdsql.org/browse/CORE-6217): [inet.cpp] Dangerous work with pointer: delete ptr; ptr=new ;  
  Note(s): Backported into v3.0.6  
  Contributor(s): Dmitry Kovalenko, Alex Peshkoff

* [CORE-6214](http://tracker.firebirdsql.org/browse/CORE-6214): Update outdated tzdata version  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6212](http://tracker.firebirdsql.org/browse/CORE-6212): Authentication plugin on server may get garbage data from client instead empty packet  
  Contributor(s): Alex Peshkoff

* [CORE-6211](http://tracker.firebirdsql.org/browse/CORE-6211): Command "ISQL -X" can not extract ROLE name when use multi-byte charset for connection  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6208](http://tracker.firebirdsql.org/browse/CORE-6208): Grant lost in security.db after backup/restore cycle  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6207](http://tracker.firebirdsql.org/browse/CORE-6207): Modifications needed to complile Firebird.pas under FPC  
  Contributor(s): Alex Peshkoff

* [CORE-6206](http://tracker.firebirdsql.org/browse/CORE-6206): VARCHAR of insufficient length used for set bind of decfloat to varchar  
  Contributor(s): Vlad Khorsun

* [CORE-6205](http://tracker.firebirdsql.org/browse/CORE-6205): Generate proper error for UNION DISTINCT with more than 255 columns  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6186](http://tracker.firebirdsql.org/browse/CORE-6186): Original content of column which is involved into ENCRYPT() is displayed as distorted view after this call  
  Contributor(s): Alex Peshkoff

* [CORE-6181](http://tracker.firebirdsql.org/browse/CORE-6181): Operations when using "SET DECFLOAT BIND BIGINT,n" with result of 11+ digits, fail with "Decimal float invalid operation"  
  Contributor(s): Alex Peshkoff

* [CORE-6174](http://tracker.firebirdsql.org/browse/CORE-6174): ibase.h is missing from nightly builds  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6170](http://tracker.firebirdsql.org/browse/CORE-6170): Cannot install on CentOS 8 because version of LibTomMath and LibNCurses library  
  Contributor(s): Alex Peshkoff

* [CORE-6166](http://tracker.firebirdsql.org/browse/CORE-6166): Problems with long object names (> 255 bytes)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6160](http://tracker.firebirdsql.org/browse/CORE-6160): SUBSTRING of non-text/-blob is described to return NONE character set in DSQL  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6159](http://tracker.firebirdsql.org/browse/CORE-6159): SUBSTRING SIMILAR is described with wrong data type in DSQL  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6130](http://tracker.firebirdsql.org/browse/CORE-6130): Creating backup to stdout using service manager is broken  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-6116](http://tracker.firebirdsql.org/browse/CORE-6116): Metadata script extracted using ISQL of a database restored from a v2.5.9 backup is invalid when table has COMPUTED BY fields  
  Note(s): Backported into v3.0.6  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6110](http://tracker.firebirdsql.org/browse/CORE-6110): 64-bit transaction IDs are not stored properly in status vector  
  Contributor(s): Ilya Eremin

* [CORE-6080](http://tracker.firebirdsql.org/browse/CORE-6080): Attempt to drop existing user randomly fails with "336723990 : record not found for user"  
  Contributor(s): Vlad Khorsun

* [CORE-6071](http://tracker.firebirdsql.org/browse/CORE-6071): Restore of encrypted backup of database with SQL dialect 1 fails  
  Contributor(s): Alex Peshkoff

* [CORE-6061](http://tracker.firebirdsql.org/browse/CORE-6061): Building with --with-builtin-tommath doesn't work  
  Contributor(s): Alex Peshkoff

* [CORE-6056](http://tracker.firebirdsql.org/browse/CORE-6056): Overflow warnings when building some collations  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6046](http://tracker.firebirdsql.org/browse/CORE-6046): Incorrect time zone parsing reads garbage in memory  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6044](http://tracker.firebirdsql.org/browse/CORE-6044): ISQL issues with increased identifier length  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6034](http://tracker.firebirdsql.org/browse/CORE-6034): The original time zone should be set to the current time zone at routine invocation  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6033](http://tracker.firebirdsql.org/browse/CORE-6033): SUBSTRING(CURRENT_TIMESTAMP) does not work  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-6019](http://tracker.firebirdsql.org/browse/CORE-6019): Wire compression does not work without MSVC 2010 runtime installation  
  Contributor(s): Vlad Khorsun

* [CORE-5976](http://tracker.firebirdsql.org/browse/CORE-5976): GBAK multi-database file restore uses wrong minimum number of pages for first database file  
  Contributor(s): Mark Rotteveel

* [CORE-5972](http://tracker.firebirdsql.org/browse/CORE-5972): External engine trigger crashing server if table have computed field  
  Note(s): Backported into v3.0.6  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5957](http://tracker.firebirdsql.org/browse/CORE-5957): Bug in SIMILAR TO when adding numeric quantifier as bound for repetetion of expression leads to empty resultset  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5931](http://tracker.firebirdsql.org/browse/CORE-5931): SIMILAR TO does not return result when invalid pattern is used  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5892](http://tracker.firebirdsql.org/browse/CORE-5892): SQL SECURITY DEFINER context is not properly evaluated for monitoring tables  
  Contributor(s): Roman Simakov

* [CORE-5697](http://tracker.firebirdsql.org/browse/CORE-5697): Conversion from numeric literals to DECFLOAT adds precision that is not originally present  
  Contributor(s): Alex Peshkoff

* [CORE-5696](http://tracker.firebirdsql.org/browse/CORE-5696): Conversion from zero numeric literals to DECFLOAT results in incorrect value  
  Contributor(s): Alex Peshkoff

* [CORE-5691](http://tracker.firebirdsql.org/browse/CORE-5691): File description on Firebird executables should be specific  
  Contributor(s): Vlad Khorsun

* [CORE-5664](http://tracker.firebirdsql.org/browse/CORE-5664): SIMILAR TO is substantially (500-700x) slower than LIKE on trivial pattern matches with VARCHAR data  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5445](http://tracker.firebirdsql.org/browse/CORE-5445): Cannot install on Debian Stretch/Testing because version of LibTomMath library  
  Contributor(s): Alex Peshkoff

* [CORE-5364](http://tracker.firebirdsql.org/browse/CORE-5364): gfix -online normal <db> (executed in session #1) does not produce error when there is a SYSDBA-owned session #2  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

* [CORE-4893](http://tracker.firebirdsql.org/browse/CORE-4893): SIMILAR-TO leads FB to crash when matching blob with size > 2Gb to a string literal  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-4874](http://tracker.firebirdsql.org/browse/CORE-4874): Infinite "SIMILAR TO" matching  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-4739](http://tracker.firebirdsql.org/browse/CORE-4739): Accent insensitive comparison: diacritical letters with DIAGONAL crossing stroke pass only test on EQUALITY to their non-accented forms  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-3858](http://tracker.firebirdsql.org/browse/CORE-3858): Very poor performance of SIMILAR TO on some arguments   
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-3380](http://tracker.firebirdsql.org/browse/CORE-3380): Disable reading from the newly created BLOB  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-2251](http://tracker.firebirdsql.org/browse/CORE-2251): GBAK doesn't return error code  
  Note(s): Backported into v3.0.6  
  Contributor(s): Alex Peshkoff

# v4.0 Beta 1 (20-Feb-2019)

## New features

* [CORE-5990](http://tracker.firebirdsql.org/browse/CORE-5990): Pool of external connections  
  Reference(s): [/doc/sql.extensions/README.external_connections_pool](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.external_connections_pool)  
  Contributor(s): Vlad Khorsun

* [CORE-5970](http://tracker.firebirdsql.org/browse/CORE-5970): Built-in cryptographic functions  
  Reference(s): [/doc/sql.extensions/README.builtin_functions.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.builtin_functions.txt)  
  Contributor(s): Alex Peshkoff

* [CORE-5953](http://tracker.firebirdsql.org/browse/CORE-5953): Statement level read consistency in read-committed transactions  
  Reference(s): [/doc/README.read_consistency.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.read_consistency.md)  
  Contributor(s): Nickolay Samofatov, Roman Simakov, Vlad Khorsun

* [CORE-5951](http://tracker.firebirdsql.org/browse/CORE-5951): Add support of batch insert and update operations  
  Reference(s): [/doc/Using_OO_API.html](https://github.com/FirebirdSQL/firebird/raw/master/doc/Using_OO_API.html)  
  Contributor(s): Alex Peshkoff

* [CORE-5808](http://tracker.firebirdsql.org/browse/CORE-5808): Support backup of encrypted databases  
  Contributor(s): Alex Peshkoff

* [CORE-5768](http://tracker.firebirdsql.org/browse/CORE-5768): Implement FILTER-clause for aggregate functions  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5620](http://tracker.firebirdsql.org/browse/CORE-5620): Add builtin functions FIRST_DAY and LAST_DAY  
  Reference(s): [/doc/sql.extensions/README.builtin_functions.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.builtin_functions.txt)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5536](http://tracker.firebirdsql.org/browse/CORE-5536): Connection compressed/encrypted status in MON$ATTACHMENTS table  
  Reference(s): [/doc/README.monitoring_tables](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.monitoring_tables)  
  Contributor(s): Alex Peshkoff

* [CORE-2022](http://tracker.firebirdsql.org/browse/CORE-2022): Built-in logical replication  
  Reference(s): [/doc/README.replication.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.replication.md)  
  Contributor(s): Dmitry Yemanov

* [CORE-909](http://tracker.firebirdsql.org/browse/CORE-909): Ability to retrieve current UTC/GMT timestamp  
  Reference(s): [/doc/sql.extensions/README.time_zone.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.time_zone.md)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-694](http://tracker.firebirdsql.org/browse/CORE-694): Support for time zones  
  Reference(s): [/doc/sql.extensions/README.time_zone.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.time_zone.md)  
  Contributor(s): Adriano dos Santos Fernandes

## Improvements

* [CORE-5973](http://tracker.firebirdsql.org/browse/CORE-5973): Handling FP overflow in DOUBLE PRECISION value when converting from DECFLOAT  
  Contributor(s): Alex Peshkoff

* [CORE-5954](http://tracker.firebirdsql.org/browse/CORE-5954): Garbage collection in intermediate record versions  
  Reference(s): [/doc/README.read_consistency.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.read_consistency.md)  
  Contributor(s): Nickolay Samofatov, Roman Simakov, Vlad Khorsun

* [CORE-5952](http://tracker.firebirdsql.org/browse/CORE-5952): Enhance restore performance of GBAK using batch API  
  Contributor(s): Alex Peshkoff

* [CORE-5948](http://tracker.firebirdsql.org/browse/CORE-5948): Make WIN_SSPI plugin produce keys for wirecrypt plugin  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5928](http://tracker.firebirdsql.org/browse/CORE-5928): Make it possible for AuthClient plugin to access authentication block from DPB  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5921](http://tracker.firebirdsql.org/browse/CORE-5921): Provide information about Global Commit Number, Commit Number of currently used database snapshot (if any) and Commit Numbers assigned to the committed transactions  
  Reference(s): [/doc/sql.extensions/README.builtin_functions.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.builtin_functions.txt)  
  Contributor(s): Vlad Khorsun

* [CORE-5887](http://tracker.firebirdsql.org/browse/CORE-5887): Allow the use of management statements in PSQL blocks  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5883](http://tracker.firebirdsql.org/browse/CORE-5883): Services version 1 cleanup  
  Contributor(s): Alex Peshkoff

* [CORE-5874](http://tracker.firebirdsql.org/browse/CORE-5874): Provide name of read-only column incorrectly referenced in UPDATE ... SET xxx  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5779](http://tracker.firebirdsql.org/browse/CORE-5779): Add support for riscv64  
  Contributor(s): Richard Jones

* [CORE-5770](http://tracker.firebirdsql.org/browse/CORE-5770): User who is allowed to manage other users must have this ability WITHOUT need to grant him RDB$ADMIN role  
  Contributor(s): Alex Peshkoff

* [CORE-5741](http://tracker.firebirdsql.org/browse/CORE-5741): Replace word "fixing" with "adjusting" in GBAK output  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5718](http://tracker.firebirdsql.org/browse/CORE-5718): Make TempCacheLimit setting database-wise  
  Contributor(s): Dmitry Yemanov

* [CORE-5705](http://tracker.firebirdsql.org/browse/CORE-5705): Store precision of DECFLOAT in RDB$FIELDS  
  Contributor(s): Alex Peshkoff

* [CORE-5647](http://tracker.firebirdsql.org/browse/CORE-5647): Increase number of formats/versions of views from 255 to 32K  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5606](http://tracker.firebirdsql.org/browse/CORE-5606): Add expression index name to exception message if computation failed  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-4529](http://tracker.firebirdsql.org/browse/CORE-4529): Allow to use index when GROUP BY on field which has DESCENDING index  
  Contributor(s): Dmitry Yemanov

* [CORE-4409](http://tracker.firebirdsql.org/browse/CORE-4409): Enhancement in precision of calculations with NUMERIC/DECIMAL  
  Contributor(s): Alex Peshkoff

* [CORE-3808](http://tracker.firebirdsql.org/browse/CORE-3808): Provide ability to return all columns using RETURNING clause  
  Reference(s): [/doc/sql.extensions/README.returning](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.returning)  
  Contributor(s): Adriano dos Santos Fernandes

## Bugfixes

* [CORE-5995](http://tracker.firebirdsql.org/browse/CORE-5995): Creator user name is empty in user trace sessions  
  Note(s): Backported into v3.0.5  
  Contributor(s): Vlad Khorsun

* [CORE-5993](http://tracker.firebirdsql.org/browse/CORE-5993): When creation of audit log file fails, there is no error message in firebird.log  
  Note(s): Backported into v3.0.5  
  Contributor(s): Vlad Khorsun

* [CORE-5991](http://tracker.firebirdsql.org/browse/CORE-5991): Trace could not work correctly with quoted file names in trace configurations  
  Note(s): Backported into v3.0.5  
  Contributor(s): Vlad Khorsun

* [CORE-5989](http://tracker.firebirdsql.org/browse/CORE-5989): iconv / libiconv 1.15 vs libc / libiconv_open | common/isc_file.cpp  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5986](http://tracker.firebirdsql.org/browse/CORE-5986): Incorrect evaluation of NULL IS [NOT] {FALSE | TRUE}  
  Note(s): Backported into v3.0.5  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5985](http://tracker.firebirdsql.org/browse/CORE-5985): Regression: ROLE does not passed in ES/EDS (specifying it in the statement is ignored)  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5982](http://tracker.firebirdsql.org/browse/CORE-5982): Error "read permission for BLOB field", when it's an input/output procedure's parameter  
  Note(s): Backported into v3.0.5  
  Contributor(s): Dmitriy Starodubov

* [CORE-5980](http://tracker.firebirdsql.org/browse/CORE-5980): Firebird crashes due to concurrent operations with expression indices  
  Note(s): Backported into v3.0.5  
  Contributor(s): Vlad Khorsun

* [CORE-5974](http://tracker.firebirdsql.org/browse/CORE-5974): Wrong result of select distinct with decfload/timezone/collated column  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5972](http://tracker.firebirdsql.org/browse/CORE-5972): External engine trigger crashing server if table have computed field  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5965](http://tracker.firebirdsql.org/browse/CORE-5965): FB3 optimizer chooses less efficient plan than FB2.5 optimizer  
  Note(s): Backported into v3.0.5  
  Contributor(s): Dmitry Yemanov

* [CORE-5959](http://tracker.firebirdsql.org/browse/CORE-5959): Firebird returns wrong time after changes of time zone  
  Note(s): Backported into v3.0.5  
  Contributor(s): Vlad Khorsun

* [CORE-5955](http://tracker.firebirdsql.org/browse/CORE-5955): Unable to init binreloc with ld >= 2.31  
  Note(s): Backported into v3.0.5  
  Contributor(s): Roman Simakov

* [CORE-5950](http://tracker.firebirdsql.org/browse/CORE-5950): Deadlock when attaching to bugchecked database  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5943](http://tracker.firebirdsql.org/browse/CORE-5943): Server crashes preparing a query with both DISTINCT/ORDER BY and non-field expression in the select list  
  Note(s): Backported into v3.0.5  
  Contributor(s): Dmitry Yemanov

* [CORE-5934](http://tracker.firebirdsql.org/browse/CORE-5934): gpre_boot fails to link using cmake, undefined reference 'dladdr' and 'dlerror'  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5930](http://tracker.firebirdsql.org/browse/CORE-5930): Bugcheck "incorrect snapshot deallocation - too few slots"  
  Contributor(s): Vlad Khorsun

* [CORE-5927](http://tracker.firebirdsql.org/browse/CORE-5927): With some non-standard authentication plugins providing correct crypt key wire anyway remains not encrypted  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5926](http://tracker.firebirdsql.org/browse/CORE-5926): Attempt to create mapping with non-ASCII user name which is encoded in SINGLE-BYTE codepage (WIN1251) leads to 'Malformed string' error  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5918](http://tracker.firebirdsql.org/browse/CORE-5918): Memory pool statistics is not accurate  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5907](http://tracker.firebirdsql.org/browse/CORE-5907): Regression: can not launch trace if its 'database' section contains regexp pattern with curvy brackets to enclose quantifier  
  Note(s): Backported into v3.0.5  
  Contributor(s): Alex Peshkoff

* [CORE-5896](http://tracker.firebirdsql.org/browse/CORE-5896): NOT NULL constraint is not synchronized after rename column  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5871](http://tracker.firebirdsql.org/browse/CORE-5871): Incorrect caching of the subquery result (procedure call) in independent queries  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5862](http://tracker.firebirdsql.org/browse/CORE-5862): Varchar computed column without explicit type does not populate RDB$CHARACTER_LENGTH  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5861](http://tracker.firebirdsql.org/browse/CORE-5861): GRANT OPTION is not checked for new object  
  Contributor(s): Roman Simakov

* [CORE-5855](http://tracker.firebirdsql.org/browse/CORE-5855): Firebird 4.0 cannot backup DB with generators which contains space in the names  
  Contributor(s): Alex Peshkoff

* [CORE-5800](http://tracker.firebirdsql.org/browse/CORE-5800): After backup/restore the indexes by expression on computed field are not working properly  
  Note(s): Backported into v3.0.5  
  Contributor(s): Dmitry Yemanov

* [CORE-5795](http://tracker.firebirdsql.org/browse/CORE-5795): ORDER BY clause on compound index may disable usage of other indices  
  Note(s): Backported into v3.0.5  
  Contributor(s): Dmitry Yemanov

* [CORE-5750](http://tracker.firebirdsql.org/browse/CORE-5750): Date-time parsing is very weak  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5728](http://tracker.firebirdsql.org/browse/CORE-5728): Field subtype of DEC_FIXED columns not returned by isc_info_sql_sub_type  
  Contributor(s): Alex Peshkoff

* [CORE-5726](http://tracker.firebirdsql.org/browse/CORE-5726): Unclear error message when inserting value exceeding max of DEC_FIXED decimal  
  Contributor(s): Alex Peshkoff

* [CORE-5717](http://tracker.firebirdsql.org/browse/CORE-5717): Reject non-constant date/time/timestamp literals  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5710](http://tracker.firebirdsql.org/browse/CORE-5710): Datatype declaration DECFLOAT without precision should use a default precision  
  Contributor(s): Alex Peshkoff

* [CORE-5700](http://tracker.firebirdsql.org/browse/CORE-5700): DECFLOAT underflow should yield zero instead of an error  
  Contributor(s): Alex Peshkoff

* [CORE-5699](http://tracker.firebirdsql.org/browse/CORE-5699): DECFLOAT should not throw exceptions when +/-NaN, +/-sNaN and +/-Infinity is used in comparisons  
  Contributor(s): Alex Peshkoff

* [CORE-5657](http://tracker.firebirdsql.org/browse/CORE-5657): Various UDF-related security vulnerabilities  
  Contributor(s): Alex Peshkoff

* [CORE-5646](http://tracker.firebirdsql.org/browse/CORE-5646): Parse error when compiling a statement causes memory leak until attachment is disconnected  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5639](http://tracker.firebirdsql.org/browse/CORE-5639): Mapping rule using WIN_SSPI plugin: windows user group conversion to firebird role does not work  
  Contributor(s): Alex Peshkoff

* [CORE-5637](http://tracker.firebirdsql.org/browse/CORE-5637): string right truncation on restore of security db  
  Contributor(s): Alex Peshkoff

* [CORE-5612](http://tracker.firebirdsql.org/browse/CORE-5612): Gradual slowdown of view operations (create, recreate or drop)  
  Contributor(s): Dmitry Yemanov

* [CORE-5611](http://tracker.firebirdsql.org/browse/CORE-5611): Higher memory consumption for prepared statements  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5593](http://tracker.firebirdsql.org/browse/CORE-5593): System function RDB$ROLE_IN_USE cannot take long role names  
  Contributor(s): Alex Peshkoff

* [CORE-5518](http://tracker.firebirdsql.org/browse/CORE-5518): Firebird UDF string2blob() may allow remote code execution  
  Contributor(s): Alex Peshkoff

* [CORE-5480](http://tracker.firebirdsql.org/browse/CORE-5480): SUBSTRING startposition smaller than 1 should be allowed  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5395](http://tracker.firebirdsql.org/browse/CORE-5395): Invalid data type for negation (minus operator)  
  Note(s): Backported into v3.0.5  
  Contributor(s): Adriano dos Santos Fernandes, Dmitry Yemanov

* [CORE-5118](http://tracker.firebirdsql.org/browse/CORE-5118): Indices on computed fields are broken after restore (all keys are NULL)  
  Note(s): Backported into v3.0.5  
  Contributor(s): Dmitry Yemanov

* [CORE-5070](http://tracker.firebirdsql.org/browse/CORE-5070): Compound index cannot be used for filtering in some ORDER/GROUP BY queries  
  Note(s): Backported into v3.0.5  
  Contributor(s): Dmitry Yemanov

* [CORE-1592](http://tracker.firebirdsql.org/browse/CORE-1592): Altering procedure parameters can lead to unrestorable database  
  Contributor(s): Adriano dos Santos Fernandes


# v4.0 Alpha 1 (23-Aug-2017)

## New features

* [CORE-5568](http://tracker.firebirdsql.org/browse/CORE-5568): SQL SECURITY feature  
  Reference(s): [/doc/sql.extensions/README.sql_security.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.sql_security.txt)  
  Contributor(s): Roman Simakov

* [CORE-5525](http://tracker.firebirdsql.org/browse/CORE-5525): Create high-precision floating point datatype named DECFLOAT  
  Reference(s): [/doc/sql.extensions/README.data_types](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.data_types)  
  Contributor(s): Alex Peshkoff

* [CORE-5488](http://tracker.firebirdsql.org/browse/CORE-5488): Timeouts for running SQL statements and idle connections  
  Reference(s): [/doc/README.session_idle_timeouts](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.session_idle_timeouts), [/doc/README.statement_timeouts](https://github.com/FirebirdSQL/firebird/raw/master/doc/README.statement_timeouts)  
  Contributor(s): Vlad Khorsun

* [CORE-5463](http://tracker.firebirdsql.org/browse/CORE-5463): Support DEFAULT context value in INSERT, UPDATE, MERGE and UPDATE OR INSERT statements  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5449](http://tracker.firebirdsql.org/browse/CORE-5449): Support GENERATED ALWAYS identity columns and OVERRIDE clause  
  Reference(s): [doc/sql.extensions/README.identity_columns.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.identity_columns.txt)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5346](http://tracker.firebirdsql.org/browse/CORE-5346): Named windows  
  Reference(s): [doc/sql.extensions/README.window_functions.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.window_functions.md)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5343](http://tracker.firebirdsql.org/browse/CORE-5343): Allow particular DBA privileges to be transferred to regular users  
  Reference(s): [doc/sql.extensions/README.builtin_functions.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.builtin_functions.txt), [doc/sql.extensions/README.ddl](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.ddl.txt)  
  Contributor(s): Alex Peshkoff

* [CORE-3647](http://tracker.firebirdsql.org/browse/CORE-3647): Frames for window functions  
  Reference(s): [doc/sql.extensions/README.window_functions.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.window_functions.md)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-2762](http://tracker.firebirdsql.org/browse/CORE-2762): New built-in function to check whether some role is implicitly active  
  Reference(s): [doc/sql.extensions/README.cumulative_roles.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.cumulative_roles.txt)  
  Contributor(s): Roman Simakov

* [CORE-1815](http://tracker.firebirdsql.org/browse/CORE-1815): Ability to grant role to another role  
  Reference(s): [doc/sql.extensions/README.cumulative_roles.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.cumulative_roles.txt)  
  Contributor(s): Roman Simakov

* [CORE-1688](http://tracker.firebirdsql.org/browse/CORE-1688): More ANSI SQL:2003 window functions (PERCENK_RANK, CUME_DIST, NTILE)  
  Reference(s): [doc/sql.extensions/README.window_functions.md](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.window_functions.md)  
  Contributor(s): Hajime Nakagami, Adriano dos Santos Fernandes

* [CORE-751](http://tracker.firebirdsql.org/browse/CORE-751): Implicitly active roles (and their permissions summarized)  
  Contributor(s): Roman Simakov

## Improvements

* [CORE-5431](http://tracker.firebirdsql.org/browse/CORE-5431): Support for DROP IDENTITY clause  
  Reference(s): [doc/sql.extensions/README.identity_columns.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.identity_columns.txt)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5430](http://tracker.firebirdsql.org/browse/CORE-5430): Support for INCREMENT option in identity columns  
  Reference(s): [doc/sql.extensions/README.identity_columns.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.identity_columns.txt)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5380](http://tracker.firebirdsql.org/browse/CORE-5380): Allow subroutines to call others subroutines and themself recursively  
  Reference(s): [doc/sql.extensions/README.subroutines.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.subroutines.txt)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5238](http://tracker.firebirdsql.org/browse/CORE-5238): Replace xinetd support with the native listener
  Contributor(s): Alex Peshkoff

* [CORE-5119](http://tracker.firebirdsql.org/browse/CORE-5119): Support autocommit mode in SET TRANSACTION statement  
  Contributor(s): Dmitry Yemanov

* [CORE-5064](http://tracker.firebirdsql.org/browse/CORE-5064): Add datatypes (VAR)BINARY(n) and BINARY VARYING(n) as aliases for (VAR)CHAR(n) CHARACTER SET OCTETS  
  Contributor(s): Dimitry Sibiryakov

* [CORE-4436](http://tracker.firebirdsql.org/browse/CORE-4436): Support for different hash algorithms in HASH system function  
  Reference(s): [doc/sql.extensions/README.builtin_functions.txt](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.builtin_functions.txt)  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-2557](http://tracker.firebirdsql.org/browse/CORE-2557): Grants on MON$ tables  
  Contributor(s): Alex Peshkoff

* [CORE-2216](http://tracker.firebirdsql.org/browse/CORE-2216): NBackup as online dump  
  Contributor(s): Roman Simakov, Vlad Khorsun

* [CORE-2192](http://tracker.firebirdsql.org/browse/CORE-2192): Extend maximum database page size to 32KB  
  Contributor(s): Dmitry Yemanov

* [CORE-2040](http://tracker.firebirdsql.org/browse/CORE-2040): Allow exception name and possibly exception text to be determined within a "WHEN ANY" error handling block  
  Reference(s): [doc/sql.extensions/README.context_variables](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.context_variables)  
  Contributor(s): Dmitry Yemanov

* [CORE-1132](http://tracker.firebirdsql.org/browse/CORE-1132): Exception context in PSQL exception handlers  
  Reference(s): [doc/sql.extensions/README.context_variables](https://github.com/FirebirdSQL/firebird/raw/master/doc/sql.extensions/README.context_variables)  
  Contributor(s): Dmitry Yemanov

* [CORE-749](http://tracker.firebirdsql.org/browse/CORE-749): Increase maximum length of object names to 63 characters  
  Contributor(s): Adriano dos Santos Fernandes

## Bugfixes

* [CORE-5545](http://tracker.firebirdsql.org/browse/CORE-5545): Wrong syntax with CREATE TRIGGER ... ON <table> used with POSITION  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5454](http://tracker.firebirdsql.org/browse/CORE-5454): INSERT into updatable view without explicit field list failed  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5408](http://tracker.firebirdsql.org/browse/CORE-5408): Result of boolean expression can not be concatenated with string literal  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5404](http://tracker.firebirdsql.org/browse/CORE-5404): Inconsistent column/line references when PSQL definitions return errors  
  Contributor(s): Adriano dos Santos Fernandes

* [CORE-5237](http://tracker.firebirdsql.org/browse/CORE-5237): Invalid handling of dot (.) and asterisk (*) in config file name and path for include clause  
  Contributor(s): Dimitry Sibiryakov

* [CORE-5223](http://tracker.firebirdsql.org/browse/CORE-5223): Double dots are prohibited in file names if access is restricted to a list of directories  
  Contributor(s): Dimitry Sibiryakov

* [CORE-5141](http://tracker.firebirdsql.org/browse/CORE-5141): Field definition allows several NOT NULL clauses  
  Contributor(s): Dimitry Sibiryakov

* [CORE-4985](http://tracker.firebirdsql.org/browse/CORE-4985): Non-privileged user can implicitly count records in a restricted table  
  Contributor(s): Dmitry Yemanov

* [CORE-4701](http://tracker.firebirdsql.org/browse/CORE-4701): Index and blob garbage collection don't take into accout data in undo log  
  Contributor(s): Dimitry Sibiryakov

* [CORE-4483](http://tracker.firebirdsql.org/browse/CORE-4483): Changed data not visible in WHEN-section if exception occured inside SP that has been called from this code  
  Contributor(s): Dimitry Sibiryakov

* [CORE-4424](http://tracker.firebirdsql.org/browse/CORE-4424): Rollback to wrong savepoint if several exception handlers on the same level are executed  
  Contributor(s): Dimitry Sibiryakov
