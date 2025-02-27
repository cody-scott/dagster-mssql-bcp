

format_file_content = """
14.0
6
1       SQLCHAR             0       40      "\t"   1     col_na                                                                                                           SQL_Latin1_General_CP1_CI_AS
2       SQLCHAR             0       40      "\t"   2     col_a                                                                                                            SQL_Latin1_General_CP1_CI_AS
3       SQLCHAR             0       400     "\t"   3     row_hash                                                                                                         SQL_Latin1_General_CP1_CI_AS
4       SQLCHAR             0       400     "\t"   4     load_uuid                                                                                                        SQL_Latin1_General_CP1_CI_AS
5       SQLCHAR             0       30      "\t"   5     load_datetime                                                                                                    ""
6       SQLCHAR             0       1       "\n"   6     should_process_replacements                                                                                      ""
""".strip()

format_file_expected = """
14.0
6
1       SQLCHAR             0       40      "\t"   1     col_na                                                                                                           ""
2       SQLCHAR             0       40      "\t"   2     col_a                                                                                                            ""
3       SQLCHAR             0       400     "\t"   3     row_hash                                                                                                         ""
4       SQLCHAR             0       400     "\t"   4     load_uuid                                                                                                        ""
5       SQLCHAR             0       30      "\t"   5     load_datetime                                                                                                    ""
6       SQLCHAR             0       1       "\n"   6     should_process_replacements                                                                                      ""
""".strip()