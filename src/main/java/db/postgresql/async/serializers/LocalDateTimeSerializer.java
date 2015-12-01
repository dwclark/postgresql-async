package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

/* tm2timestamp()
 * Convert a tm structure to a timestamp data type.
 * Note that year is _not_ 1900-based, but is an explicit full value.
 * Also, month is one-based, _not_ zero-based.
 *
 * Returns -1 on failure (overflow).
 */
// int
// tm2timestamp(struct tm * tm, fsec_t fsec, int *tzp, timestamp * result)
// {
// #ifdef HAVE_INT64_TIMESTAMP
// 	int			dDate;
// 	int64		time;
// #else
// 	double		dDate,
// 				time;
// #endif

// 	/* Julian day routines are not correct for negative Julian days */
// 	if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
// 		return -1;

// 	dDate = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - date2j(2000, 1, 1);
// 	time = time2t(tm->tm_hour, tm->tm_min, tm->tm_sec, fsec);
// #ifdef HAVE_INT64_TIMESTAMP
// 	*result = (dDate * USECS_PER_DAY) + time;
// 	/* check for major overflow */
// 	if ((*result - time) / USECS_PER_DAY != dDate)
// 		return -1;
// 	/* check for just-barely overflow (okay except time-of-day wraps) */
// 	/* caution: we want to allow 1999-12-31 24:00:00 */
// 	if ((*result < 0 && dDate > 0) ||
// 		(*result > 0 && dDate < -1))
// 		return -1;
// #else
// 	*result = dDate * SECS_PER_DAY + time;
// #endif
// 	if (tzp != NULL)
// 		*result = dt2local(*result, -(*tzp));

public class LocalDateTimeSerializer extends Serializer<LocalDateTime> {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final LocalDateTimeSerializer instance = new LocalDateTimeSerializer();

    public Class<LocalDateTime> getType() { return LocalDateTime.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.timestamp");
    }

    public LocalDateTime fromString(final String str) {
        return LocalDateTime.parse(str + "000", DATE);
    }

    public String toString(final LocalDateTime val) {
        return val == null ? null : val.format(DATE);
    }

    public LocalDateTime read(final ByteBuffer buffer, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final ByteBuffer buffer, final LocalDateTime date, final Format format) {
        throw new UnsupportedOperationException();
    }
}
