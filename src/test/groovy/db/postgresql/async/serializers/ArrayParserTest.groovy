package db.postgresql.async.serializers;

import spock.lang.*;
import java.nio.*;
import java.nio.charset.*;
import db.postgresql.async.serializers.parsers.*;

//TODO: Finish converting this to use actual serializer classes.
class ArrayParserTest extends Specification {

    final StringSerializer sser = StringSerializer.instance;
    final IntegerSerializer iser = IntegerSerializer.instance;
    final char d = ',';

    Charset utf8 = Charset.forName('UTF-8');
    CharSequence intAry = '{1,2,3}';
    ByteBuffer intAryBuffer = ByteBuffer.wrap(intAry.getBytes(utf8));
    
    CharSequence varcharAry = "{foo,bar,baz}";
    ByteBuffer varcharAryBuffer = ByteBuffer.wrap(varcharAry.getBytes(utf8));
                                              
    CharSequence varcharAryQuotes = '{"foo ","bar ","baz "}';
    ByteBuffer varcharAryQuotesBuffer = ByteBuffer.wrap(varcharAryQuotes.getBytes(utf8));
    
    CharSequence intAry2x2 = '{{1,2},{3,4}}';
    
    CharSequence intAry3x3x3 = ('{{{1,2,3},{4,5,6},{7,8,9}},' +
                                '{{11,12,13},{14,15,16},{17,18,19}},' +
                                '{{21,22,23},{24,25,26},{27,28,29}}}');
    ByteBuffer intAry3x3x3Buffer = ByteBuffer.wrap(intAry3x3x3.getBytes(utf8));

    CharSequence ary4x1 = '{{1},{2},{3},{4}}';
    CharSequence ary1x1x4x1 = '{{{{1},{2},{3},{4}}}}';
    CharSequence ary5x2 = '{{1,2},{3,4},{1,2},{3,4},{1,2}}';

    def setup() {
        SerializationContext.stringOps().encoding = utf8
        intAryBuffer = ByteBuffer.wrap('{1,2,3}'.getBytes('UTF-8'));
    }

    @Ignore
    def "Test To Array"() {
        when:
        int[] ary1 = iser.array(intAryBuffer, intAryBuffer.remaining(), d);
        then:
        ary1 == [ 1, 2, 3 ] as int[];

        when:
        String[] ary2 = sser.array(varcharAryBuffer, varcharAryBuffer.remaining(), d);
        then:
        ary2 == [ 'foo', 'bar', 'baz' ] as String[];

        when:
        String[] ary3 = sser.array(varcharAryQuotesBuffer, varcharAryQuotesBuffer.remaining(), d);
        then:
        ary3 == [ "foo ","bar ","baz " ] as String[];

        when:
        int[][][] ary4 = iser.array(intAry3x3x3Buffer, intAry3x3x3Buffer.remaining(), d);
        then:
        ary4[0][0][0] == 1;
        ary4[0][2][2] == 9;
        ary4[1][1][1] == 15;
        ary4[2][2][2] == 29;
    }

    def "Stress Array Parser"() {
        setup:
        iser.array(intAry3x3x3Buffer, intAry3x3x3Buffer.remaining(), d);
        long startAt = System.currentTimeMillis();
        for(int i = 0; i < 100_000; ++i) {
            intAry3x3x3Buffer.flip();
            iser.array(intAry3x3x3Buffer, intAry3x3x3Buffer.remaining(), d);
        }
        long endAt = System.currentTimeMillis();

        println("Total execution time in millis: ${endAt - startAt}");
    }

    @Ignore
    def "Test Arrays With Nulls"() {
        when:
        ByteBuffer buffer = ByteBuffer.wrap('{"one",null,"null"}'.getBytes(utf8));
        String[] ary = sser.array(buffer, buffer.remaining(), d);
        then:
        ary == [ 'one', null, 'null' ] as String[];
    }

    // def "Test Box Array"() {
    //     when:
    //     Box[] ary = new ArrayParser('{(1,1),(0,0);(1,1),(-1.1,-1.1)}', new BoxSerializer(session),
    //                                 Box.PGTYPE.getDelimiter()).toArray();
    //     then:
    //     ary[0] == new Box(new Point(1,1), new Point(0,0));
    //     ary[1] == new Box(new Point(1,1), new Point(-1.1,-1.1));
    // }

    @Ignore
    def "Test Dimension Parsing"() {
        setup:
        int[] dimsIntAry = new ArrayParser(intAry, iser, d).dimensions;
        int[] dimsVarcharAry = new ArrayParser(varcharAry, sser, d).dimensions;
        int[] dimsVarcharAryQuotes = new ArrayParser(varcharAryQuotes, sser, d).dimensions;
        int[] dimsAry3x3x3 = new ArrayParser(intAry3x3x3, iser, d).dimensions;
        int[] dimsAry4x1 = new ArrayParser(ary4x1, iser, d).dimensions;
        int[] dimsAry1x1x4x1 = new ArrayParser(ary1x1x4x1, iser, d).dimensions;
        int[] dimsAry5x2 = new ArrayParser(ary5x2, iser, d).dimensions;
        
        expect:
        dimsIntAry.length == 1;
        dimsIntAry[0] == 3;
        dimsVarcharAry.length == 1;
        dimsVarcharAry[0] == 3;
        dimsVarcharAryQuotes.length == 1;
        dimsVarcharAryQuotes[0] == 3;
        dimsAry3x3x3.length == 3;
        dimsAry3x3x3.every { it == 3; };
        dimsAry4x1.length == 2;
        dimsAry4x1[0] == 4;
        dimsAry4x1[1] == 1;
        dimsAry1x1x4x1[0] == 1;
        dimsAry1x1x4x1[1] == 1;
        dimsAry1x1x4x1[2] == 4;
        dimsAry1x1x4x1[3] == 1;
        dimsAry5x2.length == 2;
        dimsAry5x2[0] == 5;
        dimsAry5x2[1] == 2;
    }

    @Ignore
    def "Test Allocate Mods Single"() {
        when:
        int[] singleMod = ArrayParser.mods([ 1 ] as int[]);
        int[] singleIndexes = [ 0 ] as int[];
        ArrayParser.calculateIndexes(singleMod, singleIndexes, 0);
        
        then:
        singleMod[0] == 1;
        singleIndexes[0] == 0;

        when:
        singleMod = ArrayParser.mods([9] as int[]);
        singleIndexes = [ 0 ] as int[];
        ArrayParser.calculateIndexes(singleMod, singleIndexes, 3);
        
        then:
        singleIndexes[0] == 3;

        when:
        ArrayParser.calculateIndexes(singleMod, singleIndexes, 7);

        then:
        singleIndexes[0] == 7;
    }

    @Ignore
    def "Test Allocate Mods 2x8"() {
        when:
        int[] mods = ArrayParser.mods([2,8] as int[]);
        int[] indexes = [ 0, 0 ] as int[];
        ArrayParser.calculateIndexes(mods, indexes, 8);
        then:
        indexes[0] == 1;
        indexes[1] == 0;

        when:
        ArrayParser.calculateIndexes(mods, indexes, 0);
        then:
        indexes[0] == 0;
        indexes[1] == 0;

        when:
        ArrayParser.calculateIndexes(mods, indexes, 15);
        then:
        indexes[0] == 1;
        indexes[1] == 7;
    }

    @Ignore
    def "Test Allocate Mods 4x2x3"() {
        when:
        int[] mods = ArrayParser.mods([4,2,3] as int[]);
        int[] indexes = [ 0, 0, 0 ] as int[];
        ArrayParser.calculateIndexes(mods, indexes, 0);
        then:
        indexes == [ 0, 0, 0 ] as int[];

        when:
        ArrayParser.calculateIndexes(mods, indexes, 5)
        then:
        indexes == [ 0, 1, 2 ] as int[];

        when:
        ArrayParser.calculateIndexes(mods, indexes, 6);
        then:
        indexes == [ 1, 0, 0 ] as int[];

        when:
        ArrayParser.calculateIndexes(mods, indexes, 15);
        then:
        indexes == [ 2, 1, 0 ] as int[];

        when:
        ArrayParser.calculateIndexes(mods, indexes, 23);
        then:
        indexes == [ 3, 1, 2 ] as int[];
    }
}
