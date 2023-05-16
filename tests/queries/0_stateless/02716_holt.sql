/* Holt tests */
SELECT Holt(0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT Holt(0.5, 0.5, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT Holt(0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT Holt(0.5, 0.5) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT Holt(-1, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT Holt(2, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT Holt(0.5, -1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT Holt(0.5, 2)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT Holt(0, 0)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT Holt(0.2, 0)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT Holt(0.2, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT Holt(0.8, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT Holt(0.8, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT Holt(1, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;

/* HoltWithTime tests */
SELECT HoltWithTime(0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTime(0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTime(-1, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTime(2, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTime(0.5, -1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTime(0.5, 2)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTime(0.5, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTime(0.5, 0.5)(timestamp, value) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTime(0.5, 0.5)(value, timestamp, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTime(0, 0)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTime(0.2, 0)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTime(0.2, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTime(0.8, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTime(0.8, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTime(1, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTime(0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTime(0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number div 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTime(0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number % 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;

/* HoltWithTimeFillGaps tests */
SELECT HoltWithTimeFillGaps(0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTimeFillGaps(0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTimeFillGaps(-1, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTimeFillGaps(2, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTimeFillGaps(0.5, -1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTimeFillGaps(0.5, 2)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTimeFillGaps(0.5, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTimeFillGaps(0.5, 0.5)(timestamp, value) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTimeFillGaps(0.5, 0.5)(value, timestamp, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWithTimeFillGaps(0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number div 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError INCORRECT_DATA }
SELECT HoltWithTimeFillGaps(0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number % 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError INCORRECT_DATA }
SELECT HoltWithTimeFillGaps(0, 0)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTimeFillGaps(0.2, 0)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 3 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTimeFillGaps(0.2, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 4 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTimeFillGaps(0.8, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 5 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTimeFillGaps(0.8, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 6 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTimeFillGaps(1, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 7 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWithTimeFillGaps(0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 8 as timestamp, number * 16 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
