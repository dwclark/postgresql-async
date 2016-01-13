class Ideas {
    
    @DefineMutation
    def insertIntoNumerals(Integer arabic, String roman) {
        "insert into numerals (arabic, roman) values (${arabic}, ${roman})";
    }

    @DefineMutation
    def deleteFromNumerals(Integer id) {
        "delete from numerals where id = ${id}"
    }

    @DefineQuery
    def queryNumerals(final List accumulate, final Integer id) {
        "select * from numerals where id > ${id}" { row ->
            accumulate << row.toMap();
        }

        accumulate;
    }

    @DefineQuery(accumulate=LIST, rowsAs=MAP)
    def queryNumeralsById(final Integer id) {
        "select * from numerals where id = ${id}"
    }

    @DefineQuery(accumulate=LIST, rowsAs=MAP)
    def selectAllNumerals() {
        "select * from numerals"
    }

    @DefineTransaction(isolation=Isolation.SERIALIZABLE, mode=RwMode.READ_WRITE, deferrable=false)
    def transaction(Expando e) {
        context:
        e.first = 0;
        
        then:
        rows('select * from numerals;') { r ->
            e.first++;
        }

        then:
        mutate('insert into numerals (arabic, roman) values ($1,$2);', [ 21. 'xxi' ]) {
            i -> e.count = i;
        }

        then:
        e.second = 0;
        rows('select * from numerals;') { r ->
            e.second++;
        }

        then:
        mutate('delete from numerals where id > $1;', [ 20 ]);
    }

    @DefineTransaction //use defaults everywhere
    def simpleTransaction(final Integer arabic, final Integer idToDelete) {
        context:
        String roman = romanFromArabic(arabic);
        
        begin: insertIntoNumerals(arabic, roman);
        then: deleteFromNumerals(idToDelete);
        then: selectAllFromNumerals();
    }
}
