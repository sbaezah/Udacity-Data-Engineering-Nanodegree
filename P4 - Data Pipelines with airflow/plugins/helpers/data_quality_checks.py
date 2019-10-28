class DataChecks:
    """Data quality check statements
        Mainly consists on execute a query over the table that the user wants to test.
        The query is about to see if exists rows in the table or if the table has duplicated id.
    """
    check_empty_table = """SELECT   (CASE WHEN COUNT(*) > 0 THEN 1 
                                    ELSE 0 
                                    END) as row_result  from {}""" 
    empty_table_check_result = 1
    
    check_songplay_id_duplicate = ("""
                            SELECT MAX(t1.count_result) as max_count 
                            FROM
                                (   SELECT 
                                            COUNT(songplay_id) as count_result
                                    FROM songplays
                                    GROUP BY songplay_id) t1
                        """)    
    count_check_result = 1  
    