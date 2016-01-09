package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.types.*;
import db.postgresql.async.*;
import db.postgresql.async.enums.*;
import java.time.*;
import static db.postgresql.async.Task.*;

class TypeLoadTest extends Specification {

    @Shared Session session;
    
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
        def list = session.withTransaction { t ->
            return t.prepared('select * from fixed_numbers order by id asc;', [], { it.toList(); }); };

        expect:
        list[0] == [1, true, 42, 420, 4200, 3.14f, 3.14159265d, new Money(100_00)];
        list[1] == [2, false, 43, 430, 4300, 2.71f, 2.71828182d, new Money(37_500_00)];
    }

    def "Fixed Numbers Write"() {
        when:
        List toInsert = [ true, (short) 20, 200, 2_000_000_000_000L, Float.MAX_VALUE, Double.MAX_VALUE, new Money(123456789) ];
        int inserted = session.withTransaction { t ->
            return t.prepared('insert into fixed_numbers ' +
                              '(my_boolean, my_smallint, my_int, my_long, my_real, my_double, my_money) ' +
                              'values ($1, $2, $3, $4, $5, $6, $7);', toInsert); };
        then:
        inserted == 1;

        when:
        List list = session.withTransaction { t ->
            t.prepared('select * from fixed_numbers where id = (select max(id) from fixed_numbers);',
                       [], { it.toList(); }); }[0];
        
        then:
        list[0] > 2;
        list.size() == toInsert.size() + 1;
        toInsert == list[1..<list.size()];

        when:
        int deleted = session.withTransaction { t ->
            t.prepared('delete from fixed_numbers where id = $1;', [ list[0] ]); };

        then:
        deleted == 1;
    }
    
    def "Dates"() {
        setup:
        def list = session.withTransaction { t ->  
            return t.prepared('select * from all_dates order by id asc;', [], { it.toList(); }); }[0];

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
        int inserted = session.withTransaction { t ->
            return t.prepared('insert into all_dates (my_date, my_time, my_time_tz, my_timestamp, my_timestamp_tz) ' +
                              'values ($1, $2, $3, $4, $5);', toInsert); };
        then:
        inserted == 1;

        when:
        List list = session.withTransaction { t ->
            t.prepared('select * from all_dates where id = (select max(id) from all_dates);',
                       [], { it.toList(); }); }[0];
        
        then:
        list.size() == 6;
        toInsert[0] == list[1];
        toInsert[1] == list[2];
        toInsert[2] == list[3];
        toInsert[3] == list[4];
        toInsert[4].toInstant() == list[5].toInstant();

        when:
        int deleted = session.withTransaction { t ->
            t.prepared('delete from all_dates where id = $1;', [ list[0] ]); };

        then:
        deleted == 1;
    }

    def "Bytes"() {
        when:
        def list = session.withTransaction { t ->  
            return t.prepared('select * from binary_fields order by id asc;', [], { it.toList(); }); }[0];
        then:
        list.size() == 2;
        list[1] == [ 0xde, 0xad, 0xbe, 0xef ] as byte[];

        when:
        def toInsert = [ [ 1, 2, 3, 4, 5, 6, 7, 8 ] as byte[] ];
        int inserted = session.withTransaction { t ->
            return t.prepared('insert into binary_fields (my_bytes) values ($1) returning id;',
                              toInsert, { r -> r.single(); } ); }[0];
        then:
        inserted > 1;

        when:
        List newList = session.withTransaction { t ->
            t.prepared('select my_bytes from binary_fields where id = $1;', [inserted]) { r -> r.single(); }; };
        then:
        newList == toInsert;

        when:
        int deleted = session.withTransaction { t ->
            t.prepared('delete from binary_fields where id = $1;', [inserted]); };
        then:
        deleted == 1;
    }

    def "Enum Types"() {
        when:
        def rows = session.withTransaction { t->
            return t.prepared('select * from my_moods order by id;', [], { r -> r.toList(); }); };
        
        then:
        rows.size() == 2
        rows[0] == [ 1, DaysOfWeek.MONDAY, Moods.MAD ];
        rows[1] == [ 2, DaysOfWeek.FRIDAY, Moods.HAPPY ];

        when:
        int id = session.withTransaction { t ->
            return t.prepared('insert into my_moods (my_day_of_the_week, my_mood) values ($1, $2) returning id;',
                              [ DaysOfWeek.SUNDAY, Moods.AFRAID ], { it.single(); })[0]; };
        then:
        id > 2;

        when:
        List list = session.withTransaction { t ->
            return t.prepared('select * from my_moods where id = $1;', [ id ]) { r -> r.toList(); }[0]; };

        then:
        list == [id, DaysOfWeek.SUNDAY, Moods.AFRAID ]

        cleanup:
        session.withTransaction { t -> t.prepared('delete from my_moods where id = $1;', [id]); };
    }

    def "Xml and Json Types"() {
        setup:
        def list = session.withTransaction { t ->
            t.prepared('select * from json_and_xml;', []) { r -> r.toList(); }; }[0];

        expect:
        list.size() == 4;
        list[0] == 1
        list[1] == '<book><title>Manual</title></book>';
        list[2] == '{"number": 1, "str": "some string", "array": [ 1, 2, 3, 4, 5 ]}';
        list[3] == new Jsonb(1, '{"str": "another string", "array": [6, 7, 8, 9, 10], "number": 2}');

        when:
        def toInsert = [ '<book><title>Pride And Prejudice</title></book>',
                         '{"number": 100, "str": "another string", "array": [ 6, 7, 8 ]}',
                         new Jsonb('{"key": "value"}') ];
        def id = session.withTransaction { t->
            t.prepared('insert into json_and_xml (my_xml, my_json, my_json_b) ' +
                       'values ($1, $2, $3) returning id;', toInsert) { r -> r.single(); }; }[0];
        then:
        id > 1;

        when:
        def inserted = session.withTransaction { t ->
            t.prepared('select * from json_and_xml where id = $1;', [id]) { r -> r.toList(); } }[0];
        then:
        inserted == [id] + toInsert;

        cleanup:
        session.withTransaction { t -> t.prepared('delete from json_and_xml where id = $1', [id]); };
    }

    def "Numerics"() {
        when:
        def list = session.withTransaction { t ->
            t.prepared('select * from numerics;', []) { r -> r.toList(); } }[0];

        then:
        list.size() == 3;
        list[1] == 1234567890123.789000;
        list[2] == 0.70;

        when:
        def toInsert = [ 175123.123456, 3.14 ];
        def id = session.withTransaction { t ->
            t.prepared('insert into numerics (my_numeric, my_money) values ($1, $2) returning id;',
                       toInsert) { r -> r.single(); }; }[0];
        
        then:
        id > 1;

        when:
        def inserted = session.withTransaction { t ->
            t.prepared('select * from numerics where id = $1;', [id]) { r -> r.toList(); } }[0];
        then:
        inserted == [id] + toInsert;

        cleanup:
        session.withTransaction { t ->
            t.prepared('delete from numerics where id = $1;', [id]); }; 
    }

    def "Character Types"() {
        setup:
        def shouldBe = [1, 'some chars     ', 'something that varies',
                        'en arche en ho logos, kai ho logos en pros ton theon...']
        when:
        def list = session.withTransaction { t ->
            t.prepared('select * from character_types;', []) { r -> r.toList(); } }[0];
        then:
        list.size() == 4;
        list == shouldBe;

        when:
        def toInsert = [ '123456789012345', 'stuff', 'more stuff' ];
        def id = session.withTransaction { t ->
            t.prepared('insert into character_types (my_char, my_varchar, my_text) values ' +
                       '($1, $2, $3) returning id', toInsert) { r -> r.single(); } }[0];
        then:
        id > 1;

        when:
        def found = session.withTransaction { t ->
            t.prepared('select * from character_types where id = $1', [id]) { r -> r.toList(); } }[0];
        then:
        [ id ] + toInsert == found;

        cleanup:
        session.withTransaction { t ->
            t.prepared('delete from character_types where id = $1', [id]); };
    }

    def "Interval Types"() {
        setup:
        Duration duration = Duration.ofHours(4) + Duration.ofMinutes(5) + Duration.ofSeconds(6);
        def shouldBe = [ 1, new Interval(Period.of(1, 2, 3), duration) ];

        when:
        def list = session.withTransaction { t ->
            t.prepared('select * from intervals;', []) { r -> r.toList(); } }[0];
        then:
        list.size() == 2;
        list == shouldBe;

        when:
        def i = new Interval(Period.of(0, 7, 5), Duration.ofSeconds(100));
        def id = session.withTransaction { t ->
            t.prepared('insert into intervals (my_interval) values ($1) returning id;', [i]) { r -> r.single(); }; }[0];
        then:
        id > 1;

        when:
        def found = session.withTransaction { t ->
            t.prepared('select * from intervals where id = $1', [id]) { r -> r.toList(); } }[0];
        then:
        found[1] == i;

        cleanup:
        session.withTransaction { t ->
            t.prepared('delete from intervals where id = $1;', [id]); };
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
        when:
        def list = session.withTransaction { t ->
            t.prepared('select * from geometry_types;', []) { r -> r.toList(); }; }[0];
        then:
        list.size() == 9;
        list == first;

        when:
        def toInsert = [ new Point(1.1,1.1),
                         new Line(1.1,2.1,3.1), new LineSegment(new Point(1.1,2.1), new Point(3.1,4.1)),
                         new Box(new Point(3.1,4.1), new Point(1.1,2.1)),
                         new Path(true, new Point(0.1,0.1), new Point(1.1,1.1), new Point(1.1,0.1)),
                         new Path(false, new Point(0.1,0.1), new Point(1.1,1.1), new Point(1.1,0.1)),
                         new Polygon(new Point(0.1,0.1), new Point(1.1,1.1), new Point(1.1,0.1)),
                         new Circle(new Point(1.1,1.1), 5.1) ];
        def id = session.withTransaction { t ->
            t.prepared('insert into geometry_types (my_point, my_line, my_lseg, my_box, my_closed_path, my_open_path, my_polygon, my_circle) ' +
                       'values ($1,$2,$3,$4,$5,$6,$7,$8) returning id', toInsert) { r -> r.single(); } }[0];
        then:
        id > 1;

        when:
        def inserted = session.withTransaction { t ->
            t.prepared('select * from geometry_types where id = $1;', [id]) { r -> r.toList(); } }[0];
        then:
        [id] + toInsert == inserted;

        cleanup:
        session.withTransaction { t -> t.prepared('delete from geometry_types where id = $1;', [id]); };
    }

    def "UUID and BitSet"() {
        setup:
        BitSet bitSet = new BitSet(5);
        bitSet.set(0); bitSet.set(2); bitSet.set(4);
        def first = [ 1, bitSet, UUID.fromString('aa81b166-c60f-4e4e-addb-17414a652733') ];
            
        when:
        def list = session.withTransaction { t ->
            t.prepared('select * from extended_types;', []) { r -> r.toList(); }; }[0];
        then:
        list.size() == 3;
        list == first;

        when:
        BitSet two = new BitSet(12);
        two.set(1); two.set(3); two.set(5); two.set(7); two.set(9); two.set(11);
        def toInsert = [ two, UUID.randomUUID() ];
        def id = session.withTransaction { t ->
            t.prepared('insert into extended_types (my_bits, my_uuid) values ($1, $2) returning id;', toInsert) { r -> r.single(); }; }[0];
        then:
        id > 1;

        when:
        def inserted = session.withTransaction { t ->
            t.prepared('select my_bits, my_uuid from extended_types where id = $1;', [id]) { r -> r.toList() }; }[0];
        then:
        toInsert == inserted;

        cleanup:
        session.withTransaction { t -> t.prepared('delete from extended_types where id = $1;', [id]); };
    }

    def "Network Types"() {
        setup:
        def original = [ 1, MacAddr.fromString('08:00:2b:01:02:03'),
                         Inet.fromString('10.10.23.1/32'), Inet.fromString('192.168.10.0/24', true) ];

        when:
        def found = session.withTransaction { t ->
            t.prepared('select * from network_types', []) { r -> r.toList(); }; }[0];
        then:
        found == original;

        when:
        def toInsert = [ MacAddr.fromString('18:10:3b:11:12:13'),
                         Inet.fromString('2001:4f8:3:ba:2e0:81ff:fe22:0/112'),
                         Inet.fromString('2001:4f8:3:ba::/64', true) ];
        
        def id = session.withTransaction { t ->
            t.prepared('insert into network_types (my_macaddr, my_inet, my_cidr) ' +
                       'values ($1, $2, $3) returning id;', toInsert) { r -> r.single(); }; }[0];
        then:
        id > 1;

        when:
        def inserted = session.withTransaction { t ->
            t.prepared('select * from network_types where id = $1;', [id]) { r -> r.toList(); }; }[0];
        then:
        inserted == [id] + toInsert;

        cleanup:
        session.withTransaction { t ->
        t.prepared('delete from network_types where id = $1;', [id]); };
    }

    def "Arrays"() {
        setup:
        def original = [ 1, [ 1, 2, 3, 4, 5] as int[], ['one', 'two', 'three', 'four', 'five'] as String[] ];
        
        when:
        def found = session.withTransaction { t ->
            t.prepared('select * from my_arrays', []) { r ->
                def iter = r.iterator();
                [ iter.nextInt(), iter.nextArray(int.class), iter.next() ]; }; }[0];
        then:
        found[0] == original[0];
        found[1] == original[1];
        found[2] == original[2]

        when:
        def multiInt = [ [ 1, 2, 3 ], [ 4, 5, 6 ] ] as int[][];
        def multiString = [ [ '1', '2', '3' ], [ '4', '5', '6' ] ] as String[][];
        def toInsert = [ multiInt, multiString ];
        def id = session.withTransaction { t ->
            t.prepared('insert into my_arrays (int_array, string_array) values ($1, $2) returning id;', toInsert) { r -> r.single(); } }[0];

        then:
        id > 0;

        when:
        def inserted = session.withTransaction { t ->
            t.prepared('select * from my_arrays where id = $1;', [id]) { r ->
                def iter = r.iterator();
                [ iter.next(), iter.nextArray(int.class), iter.next() ] } }[0];
        then:
        inserted[1] == multiInt;
        inserted[2] == multiString;

        cleanup:
        session.withTransaction { t -> t.prepared('delete from my_arrays where id = $1', [id]); };
    }

    def "Record"() {
        when:
        def list = session.withTransaction { t ->
            return t.prepared('select fixed_numbers from fixed_numbers order by id asc;', [], { r -> r.single(); }); };
        then:
        list.size() == 2;
        list[0] instanceof Record;

        when:
        def person = session.withTransaction { t ->
            return t.prepared('select the_person from persons;', []) { r-> r.single(); }; }[0];
        then:
        person instanceof Record;
    }

    def "Ranges"() {
        setup:
        def original = [ 1, new Range.Int4(Range.Bound.INCLUSIVE, 2, 21, Range.Bound.EXCLUSIVE) ];
        
        when:
        def list = session.withTransaction { t ->
            t.prepared('select * from ranges', []) { r -> r.toList(); }; }[0];
        then:
        list == original;
        
        when:
        def toInsert = [ new Range.Int4(Range.Bound.INCLUSIVE, -99, 201, Range.Bound.EXCLUSIVE) ];
        def id = session.withTransaction { t ->
            t.prepared('insert into ranges (int_range) values ($1) returning id;', toInsert) { r -> r.single(); }; }[0];
        then:
        id > 1;

        when:
        def inserted = session.withTransaction { t ->
            t.prepared('select * from ranges where id = $1;', [id]) { r -> r.toList(); }; }[0];
        then:
        [id] + toInsert == inserted;

        cleanup:
        session.withTransaction { t -> t.prepared('delete from ranges where id = $1;', [id]); };
    }
}
