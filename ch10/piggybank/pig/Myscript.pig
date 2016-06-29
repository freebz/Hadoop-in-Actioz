/*
 * Myscript.pig
 * Another line of comment
 */
log = LOAD '$input' AS (user, time, query);
lmt = LIMIT log $size; -- Only show 4 tuples
DUMP lmt;
-- End of program
