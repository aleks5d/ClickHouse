/* HoltWintersMultiply tests */
SELECT HoltWintersMultiply(0.5, 0.5, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0.5, 0.5, 0.5, 1, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(-1, 0.5, 0.5, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(2, 0.5, 0.5, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0.5, -1, 0.5, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0.5, 2, 0.5, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0.5, 0.5, -1, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0.5, 0.5, 2, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0.5, 0.5, 0.5, 0)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0.5, 0.5, 0.5, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0.5, 0.5, 0.5, 4) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiply(0, 0, 0, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiply(0.2, 0.5, 0.8, 3)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiply(1, 1, 1, 5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiply(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number % 4 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiply(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;

/* HoltWintersAdditional tests */
SELECT HoltWintersAdditional(0.5, 0.5, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0.5, 0.5, 0.5, 1, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(-1, 0.5, 0.5, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(2, 0.5, 0.5, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0.5, -1, 0.5, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0.5, 2, 0.5, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0.5, 0.5, -1, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0.5, 0.5, 2, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0.5, 0.5, 0.5, 0)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0.5, 0.5, 0.5, 0.5)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0.5, 0.5, 0.5, 4) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditional(0, 0, 0, 1)(value) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditional(0.2, 0.5, 0.8, 3)(value) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditional(1, 1, 1, 5)(value) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditional(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number % 4 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditional(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditional(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number div 2 as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditional(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number % 2 as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;

/* HoltWintersMultiplyWithTime tests */
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 1, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(-1, 0.5, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(2, 0.5, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, -1, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, 2, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, -1, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 2, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 0)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 4)(timestamp, value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 4)(value, timestamp, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTime(0, 0, 0, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTime(0.2, 0.5, 0.8, 3)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTime(1, 1, 1, 5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number % 4 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number % 4 as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTime(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number div 4 as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;

/* HoltWintersAdditionalWithTime tests */
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 1, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(-1, 0.5, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(2, 0.5, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, -1, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, 2, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, -1, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 2, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 0)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 4)(timestamp, value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 4)(value, timestamp, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTime(0, 0, 0, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTime(0.2, 0.5, 0.8, 3)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTime(1, 1, 1, 5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number % 4 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number % 4 as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTime(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number div 4 as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;

/* HoltWintersMultiplyWithTimeFillGaps tests */
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 1, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(-1, 0.5, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(2, 0.5, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, -1, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 2, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, -1, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 2, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 0)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 4)(timestamp, value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number div 2 as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError INCORRECT_DATA }
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number % 2 as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError INCORRECT_DATA }
SELECT HoltWintersMultiplyWithTimeFillGaps(0, 0, 0, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTimeFillGaps(0.2, 0.5, 0.8, 3)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTimeFillGaps(1, 1, 1, 5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 3 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number % 4 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersMultiplyWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 3 as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;

/* HoltWintersAdditionalWithTimeFillGaps tests */
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 1, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(-1, 0.5, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(2, 0.5, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, -1, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 2, 0.5, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, -1, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 2, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 0)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 0.5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 4)(timestamp, value) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp, timestamp) over (ORDER BY timestamp ASC) from (SELECT number as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError BAD_ARGUMENTS }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number div 2 as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError INCORRECT_DATA }
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number % 2 as timestamp, number as value from numbers_mt(100)) OFFSET 99; -- { serverError INCORRECT_DATA }
SELECT HoltWintersAdditionalWithTimeFillGaps(0, 0, 0, 1)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTimeFillGaps(0.2, 0.5, 0.8, 3)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTimeFillGaps(1, 1, 1, 5)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 3 as timestamp, number * 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 2 as timestamp, number % 4 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
SELECT HoltWintersAdditionalWithTimeFillGaps(0.5, 0.5, 0.5, 4)(value, timestamp) over (ORDER BY timestamp ASC) from (SELECT number * 3 as timestamp, number % 2 + rand() % 100 / 100 - 0.5 as value from numbers_mt(100)) OFFSET 99;
