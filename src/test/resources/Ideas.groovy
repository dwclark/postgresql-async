class Ideas {
    
    @DefineUpdate
    Integer insertIntoNumerals(Integer arabic, String roman) {
        sql: "insert into numerals (arabic, roman) values (${arabic}, ${roman})";
    }

    @DefineUpdate
    Integer deleteFromNumerals(Integer id) {
        sql: "delete from numerals where id = ${id}";
    }

    @DefineQuery
    List queryNumerals(final List accumulate, final Integer id) {
        sql: "select * from numerals where id > ${id}" { row ->
            accumulate << row.toMap();
        }

        return accumulate;
    }

    @DefineQuery(accumulate=LIST, rowsAs=MAP)
    List<Map> queryNumeralsById(final Integer id) {
        sql: "select * from numerals where id = ${id}";
    }

    @DefineQuery(accumulate=LIST, rowsAs=MAP)
    List<Map> selectAllNumerals() {
        sql: "select * from numerals";
    }

    @DefineTransaction(isolation=Isolation.SERIALIZABLE, mode=RwMode.READ_WRITE, deferrable=false)
    def transaction(Expando e) {
        context:
        e.first = 0;
        
        then:
        query('select * from numerals;') { r ->
            e.first++;
        }

        then:
        update('insert into numerals (arabic, roman) values ($1,$2);', [ 21, 'xxi' ]) {
            i -> e.count = i;
        }

        then:
        e.second = 0;
        query('select * from numerals;') { r ->
            e.second++;
        }

        then:
        update('delete from numerals where id > $1;', [ 20 ]);
    }

    @DefineTransaction //use defaults everywhere
    def simpleTransaction(final Integer arabic, final Integer idToDelete) {
        setup:
        String roman = romanFromArabic(arabic);
        
        then: insertIntoNumerals(arabic, roman);
        then: deleteFromNumerals(idToDelete);
        then: selectAllFromNumerals();
    }
    
    //call like:
    session.execute(insertIntoNumerals(100, 'c'));
}
