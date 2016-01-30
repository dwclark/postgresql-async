package db.postgresql.async;

import java.util.function.Consumer;
import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.types.*;
import db.postgresql.async.*;
import db.postgresql.async.enums.*;
import java.time.*;
import static db.postgresql.async.Task.*;
import static db.postgresql.async.Task.Prepared.*;

class TypeLoadTest extends Specification {

    private static Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }
    
    def "Load Types"() {
        expect:
        session.sessionInfo.registry.pgType(23);
        session.sessionInfo.registry.pgType(1043);
    }

    def "Fixed Numbers"() {
        setup:
        def list = session(applyRows('select * from fixed_numbers order by id asc;') { it.toList(); }).get()

        expect:
        list[0] == [1, true, 42, 420, 4200, 3.14f, 3.14159265d, new Money(100_00)];
        list[1] == [2, false, 43, 430, 4300, 2.71f, 2.71828182d, new Money(37_500_00)];
    }

    def "Fixed Numbers Write"() {
        when:
        List toInsert = [ true, (short) 20, 200, 2_000_000_000_000L, Float.MAX_VALUE,
                          Double.MAX_VALUE, new Money(123456789) ];
        int inserted = session(count('insert into fixed_numbers ' +
                                     '(my_boolean, my_smallint, my_int, my_long, my_real, my_double, my_money) ' +
                                     'values ($1, $2, $3, $4, $5, $6, $7);', toInsert)).get()
        then:
        inserted == 1;

        when:
        List list = session(applyRows('select * from fixed_numbers where id = (select max(id) from fixed_numbers);',
                                      { it.toList(); })).get()[0];
        
        then:
        list[0] > 2;
        list.size() == toInsert.size() + 1;
        toInsert == list[1..<list.size()];

        when:
        int deleted = session(count('delete from fixed_numbers where id = $1;', [ list[0] ])).get();

        then:
        deleted == 1;
    }
    
    def "Dates"() {
        setup:
        def list = session(applyRows('select * from all_dates order by id asc;',
                                     { r -> r.toList(); })).get().head();

        expect:
        list[0] == 1;
        list[1] == LocalDate.of(1999, 1, 8);
        list[2] == LocalTime.of(4, 5, 6, 789_000_000);
        list[3] == OffsetTime.of(4, 5, 6, 789_000_000, ZoneOffset.of('-06:00:00'));
        list[4] == LocalDateTime.of(1999, 1, 8, 4, 5, 6, 789_000_000);
        list[5].isEqual(OffsetDateTime.of(1999, 1, 8, 4, 5, 6, 789_000_000, ZoneOffset.of('-06:00:00')));
    }

    def "Dates Write"() {
        when:
        List toInsert = [ LocalDate.of(2010, 10, 31),
                          LocalTime.of(13, 25, 17, 900_000_000),
                          OffsetTime.of(13, 25, 17, 900_000_000, ZoneOffset.of('-04:00:00')),
                          LocalDateTime.of(2010, 10, 31, 13, 25, 17, 900_000_000),
                          OffsetDateTime.of(2010, 10, 31, 13, 25, 17, 900_000_000, ZoneOffset.of('-04:00:00')) ];
        int inserted = session(count('insert into all_dates (my_date, my_time, my_time_tz, my_timestamp, my_timestamp_tz) ' +
                                     'values ($1, $2, $3, $4, $5);', toInsert)).get();
        then:
        inserted == 1;

        when:
        List list = session(applyRows('select * from all_dates where id = (select max(id) from all_dates);',
                                      { r -> r.toList(); })).get().head();
        
        then:
        list.size() == 6;
        toInsert[0] == list[1];
        toInsert[1] == list[2];
        toInsert[2] == list[3];
        toInsert[3] == list[4];
        toInsert[4].toInstant() == list[5].toInstant();

        when:
        int deleted = session(count('delete from all_dates where id = $1;', [ list[0] ])).get();

        then:
        deleted == 1;
    }

    def "Bytes"() {
        setup:
        def toInsert = [ 1, 2, 3, 4, 5, 6, 7, 8 ] as byte[];
        
        when:
        def e = session(transaction(new Expando())
                        << { e ->
                            applyRows('select * from binary_fields order by id asc;') { row ->
                                Row.Iterator iter = row.iterator();
                                iter.nextInt();
                                e.original = iter.next();
                            }
                        }
                        << { e ->
                            acceptRows('insert into binary_fields (my_bytes) values ($1) returning id;', [ toInsert ]) { row ->
                                e.id = row.single();
                            }
                        }
                        << { e ->
                            acceptRows('select my_bytes from binary_fields where id = $1;', [e.id]) { row ->
                                e.newBytes = row.single();
                            }
                        }
                        << { e ->
                            count('delete from binary_fields where id = $1;', [e.id]) { i ->
                                e.deleteCount = i;
                            }
                        }).get();
        
        then:
        e.original == [ 0xde, 0xad, 0xbe, 0xef ] as byte[];
        e.newBytes == toInsert;
        e.deleteCount == 1;
    }

    def "Enum Types"() {
        setup:
        def e = session(transaction(new Expando())
                        << { e ->
                            e.initial = [];
                            acceptRows('select * from my_moods order by id;', []) { row ->
                                e.initial << row.toList();
                            }
                        }
                        << { e ->
                            def args = [ DaysOfWeek.SUNDAY, Moods.AFRAID ];
                            acceptRows('insert into my_moods (my_day_of_the_week, my_mood) values ($1, $2) returning id;', args) { row ->
                                e.id = row.single();
                            }
                        }
                        << { e ->
                            acceptRows('select * from my_moods where id = $1;', [ e.id ]) { row ->
                                e.list = row.toList();
                            }
                        }
                        << { e ->
                            count('delete from my_moods where id = $1;', [ e.id ]);
                        }).get();
        
        expect:
        e.initial.size() == 2
        e.initial[0] == [ 1, DaysOfWeek.MONDAY, Moods.MAD ];
        e.initial[1] == [ 2, DaysOfWeek.FRIDAY, Moods.HAPPY ];
        e.id > 2;
        e.list == [ e.id, DaysOfWeek.SUNDAY, Moods.AFRAID ];
    }

    def "Xml and Json Types"() {
        setup:
        def select = { e ->
            acceptRows('select * from json_and_xml;', NO_ARGS) { row ->
                e.initial = row.toList(); }; };
        
        def toInsert = [ '<book><title>Pride And Prejudice</title></book>',
                         '{"number": 100, "str": "another string", "array": [ 6, 7, 8 ]}',
                         new Jsonb('{"key": "value"}') ];
        def insert = { e ->
            acceptRows('insert into json_and_xml (my_xml, my_json, my_json_b) ' +
                       'values ($1, $2, $3) returning id;', toInsert) { row -> e.id = row.single(); } }

        def reSelect = { e ->
            acceptRows('select * from json_and_xml where id = $1;', [e.id]) { row -> e.found = row.toList(); } };
        
        def delete = { e ->
            count('delete from json_and_xml where id = $1', [e.id]) };

        def e = session(transaction(new Expando())
                        << select << insert << reSelect << delete).get();
        expect:
        e.initial.size() == 4;
        e.initial[0] == 1
        e.initial[1] == '<book><title>Manual</title></book>';
        e.initial[2] == '{"number": 1, "str": "some string", "array": [ 1, 2, 3, 4, 5 ]}';
        e.initial[3] == new Jsonb(1, '{"str": "another string", "array": [6, 7, 8, 9, 10], "number": 2}');
        e.id > 1;
        e.found == [ e.id ] + toInsert;
    }

    def "Numerics"() {
        setup:
        def select = { e ->
            acceptRows('select * from numerics;', NO_ARGS) { row ->
                e.initial = row.toList(); }; };
        
        def toInsert = [ 175123.123456, 3.14 ];

        def insert = { e ->
            acceptRows('insert into numerics (my_numeric, my_money) values ($1, $2) returning id;', toInsert) { row ->
                e.id = row.single(); }; };
        
        def reSelect = { e ->
            acceptRows('select * from numerics where id = $1;', [e.id]) { row ->
                e.found = row.toList(); }; };

        def delete = { e ->
            count('delete from numerics where id = $1;', [e.id]); };

        def e = session(transaction(new Expando())
                        << select << insert << reSelect << delete).get();
        expect:
        e.initial.size() == 3;
        e.initial[1] == 1234567890123.789000;
        e.initial[2] == 0.70;
        e.id > 1;
        e.found == [e.id] + toInsert;
    }

    def "Character Types"() {
        setup:
        def toInsert = [ '123456789012345', 'stuff', 'more stuff' ];

        def select = { e -> acceptRows('select * from character_types;', NO_ARGS) { r -> e.initial = r.toList(); }; };

        def insert = { e ->
            def sql = ('insert into character_types (my_char, my_varchar, my_text) values ' +
                       '($1, $2, $3) returning id');
            acceptRows(sql, toInsert) { r -> e.id = r.single(); }; };

        def reSelect = { e ->
            acceptRows('select * from character_types where id = $1', [e.id]) { r -> e.found = r.toList(); }; };

        def e = session(transaction(new Expando())
                        << select << insert << reSelect << { e -> rollback() }).get();
        expect:
        e.initial.size() == 4;
        e.initial == [1, 'some chars     ', 'something that varies',
                      'en arche en ho logos, kai ho logos en pros ton theon...'];
        e.id > 1;
        [ e.id ] + toInsert == e.found;
    }

    def "Interval Types"() {
        setup:
        Duration duration = Duration.ofHours(4) + Duration.ofMinutes(5) + Duration.ofSeconds(6);
        def shouldBe = [ 1, new Interval(Period.of(1, 2, 3), duration) ];
        def select = { e ->
            acceptRows('select * from intervals;', NO_ARGS) { r -> e.initial = r.toList(); }; };
        def i = new Interval(Period.of(0, 7, 5), Duration.ofSeconds(100));
        def insert = { e ->
            acceptRows('insert into intervals (my_interval) values ($1) returning id;',
                       [ i ]) { r -> e.id = r.single(); }; };

        def reSelect = { e ->
            acceptRows('select * from intervals where id = $1', [e.id]) { r -> e.found = r.toList(); }; };

        def back = { e -> rollback(); };
        
        def e = session(transaction(new Expando())
                        << select << insert << reSelect << back).get();
        expect:
        e.initial.size() == 2;
        e.initial == shouldBe;
        e.id > 1;
        e.found[1] == i;
    }

    def "Geometry Types"() {
        setup:
        def first = [1, new Point(1.0,1.0),
                     new Line(1.0,2.0,3.0), new LineSegment(new Point(1.0,2.0), new Point(3.0,4.0)),
                     new Box(new Point(3.0,4.0), new Point(1.0,2.0)),
                     new Path(true, new Point(0.0,0.0), new Point(1.0,1.0), new Point(1.0,0.0)),
                     new Path(false, new Point(0.0,0.0), new Point(1.0,1.0), new Point(1.0,0.0)),
                     new Polygon(new Point(0.0,0.0), new Point(1.0,1.0), new Point(1.0,0.0)),
                     new Circle(new Point(1.0,1.0), 5.0) ];
        def toInsert = [ new Point(1.1,1.1),
                         new Line(1.1,2.1,3.1), new LineSegment(new Point(1.1,2.1), new Point(3.1,4.1)),
                         new Box(new Point(3.1,4.1), new Point(1.1,2.1)),
                         new Path(true, new Point(0.1,0.1), new Point(1.1,1.1), new Point(1.1,0.1)),
                         new Path(false, new Point(0.1,0.1), new Point(1.1,1.1), new Point(1.1,0.1)),
                         new Polygon(new Point(0.1,0.1), new Point(1.1,1.1), new Point(1.1,0.1)),
                         new Circle(new Point(1.1,1.1), 5.1) ];

        def select = { e -> acceptRows('select * from geometry_types;', NO_ARGS) { r -> e.initial = r.toList(); }; };
        def insert = { e ->
            acceptRows('insert into geometry_types (my_point, my_line, my_lseg, my_box, my_closed_path, my_open_path, my_polygon, my_circle) ' +
                       'values ($1,$2,$3,$4,$5,$6,$7,$8) returning id', toInsert) { r -> e.id = r.single(); }; };
        def reSelect = { e ->
            acceptRows('select * from geometry_types where id = $1;', [e.id]) { r -> e.found = r.toList(); }; };
        def back = { e -> rollback(); }
        def e = session(transaction(new Expando())
                        << select << insert << reSelect << back).get();
        
        expect:
        e.initial.size() == 9;
        e.initial == first;
        e.id > 1;
        [e.id] + toInsert == e.found;
    }

    def "UUID and BitSet"() {
        setup:
        BitSet bitSet = new BitSet(5);
        bitSet.set(0); bitSet.set(2); bitSet.set(4);
        def first = [ 1, bitSet, UUID.fromString('aa81b166-c60f-4e4e-addb-17414a652733') ];

        def select = { e ->
            acceptRows('select * from extended_types;', NO_ARGS) { r -> e.initial = r.toList(); }; };
        BitSet two = new BitSet(12);
        two.set(1); two.set(3); two.set(5); two.set(7); two.set(9); two.set(11);
        def toInsert = [ two, UUID.randomUUID() ];
        def insert = { e ->
            acceptRows('insert into extended_types (my_bits, my_uuid) values ($1, $2) returning id;',
                       toInsert) { r -> e.id = r.single(); }; };
        def reSelect = { e ->
            acceptRows('select my_bits, my_uuid from extended_types where id = $1;', [e.id]) { r -> e.found = r.toList() }; };
        def back = { e -> rollback(); }
        def e = session(transaction(new Expando())
                        << select << insert << reSelect << back).get();
        
        expect:
        e.initial == first;
        e.id > 1;
        toInsert == e.found;
    }

    def "Network Types"() {
        setup:
        def original = [ 1, MacAddr.fromString('08:00:2b:01:02:03'),
                         Inet.fromString('10.10.23.1/32'), Inet.fromString('192.168.10.0/24', true) ];
        def select = { e ->
            acceptRows('select * from network_types', NO_ARGS) { r -> e.initial = r.toList(); }; };

        def toInsert = [ MacAddr.fromString('18:10:3b:11:12:13'),
                         Inet.fromString('2001:4f8:3:ba:2e0:81ff:fe22:0/112'),
                         Inet.fromString('2001:4f8:3:ba::/64', true) ];

        def insert = { e ->
            acceptRows('insert into network_types (my_macaddr, my_inet, my_cidr) ' +
                       'values ($1, $2, $3) returning id;', toInsert) { r -> e.id = r.single(); }; };

        def reSelect = { e ->
            acceptRows('select * from network_types where id = $1;', [e.id]) { r -> e.found = r.toList(); }; };

        def back = { e -> rollback(); };

        def e = session(transaction(new Expando())
                        << select << insert << reSelect << back).get();
        
        expect:
        e.initial == original;
        e.id > 1;
        e.found == [e.id] + toInsert;
    }

    def "Arrays"() {
        setup:
        def original = [ 1, [ 1, 2, 3, 4, 5] as int[], ['one', 'two', 'three', 'four', 'five'] as String[] ];

        def select = { e ->
            acceptRows('select * from my_arrays', []) { r ->
                def iter = r.iterator();
                e.initial = [ iter.nextInt(), iter.nextArray(int.class), iter.next() ]; }; };

        def multiInt = [ [ 1, 2, 3 ], [ 4, 5, 6 ] ] as int[][];
        def multiString = [ [ '1', '2', '3' ], [ '4', '5', '6' ] ] as String[][];
        def toInsert = [ multiInt, multiString ];

        def insert = { e ->
            acceptRows('insert into my_arrays (int_array, string_array) values ($1, $2) returning id;',
                       toInsert) { r -> e.id = r.single(); } };

        def reSelect = { e ->
            acceptRows('select * from my_arrays where id = $1;', [e.id]) { r ->
                def iter = r.iterator();
                e.found = [ iter.next(), iter.nextArray(int.class), iter.next() ]; }; };

        def back = { e -> rollback(); }
        
        def e = session(transaction(new Expando())
                        << select << insert << reSelect << back).get();
        expect:
        e.initial[0] == original[0];
        e.initial[1] == original[1];
        e.initial[2] == original[2]
        e.id > 0;
        e.found[1] == multiInt;
        e.found[2] == multiString;
    }

    def "Record"() {
        setup:
        def numbers = { e ->
            e.numbers = [];
            acceptRows('select fixed_numbers from fixed_numbers order by id asc;', NO_ARGS) { r -> e.numbers << r.single(); }; };

        def persons = { e ->
            acceptRows('select the_person from persons;', NO_ARGS) { r -> e.person = r.single(); }; };

        def e = session(transaction(new Expando())
                        << numbers << persons).get();
        
        expect:
        e.numbers.size() == 2;
        e.numbers[0] instanceof Record;
        e.person instanceof Record;
    }

    def "Ranges"() {
        setup:
        def original = [ 1, new Range.Int4(Range.Bound.INCLUSIVE, 2, 21, Range.Bound.EXCLUSIVE) ];
        def toInsert = [ new Range.Int4(Range.Bound.INCLUSIVE, -99, 201, Range.Bound.EXCLUSIVE) ];
        
        def select = { e ->
            acceptRows('select * from ranges', NO_ARGS) { r -> e.initial = r.toList(); }; };

        def insert = { e ->
            acceptRows('insert into ranges (int_range) values ($1) returning id;',
                       toInsert) { r -> e.id = r.single(); }; };

        def reSelect = { e ->
            acceptRows('select * from ranges where id = $1;', [e.id]) { r -> e.found = r.toList(); }; };

        def back = { e -> rollback(); };
        
        def e = session(transaction(new Expando())
                        << select << insert << reSelect << back).get();
        expect:
        e.initial == original;
        e.id > 1;
        [e.id] + toInsert == e.found;
    }
}
