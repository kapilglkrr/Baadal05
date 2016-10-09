package cloud;
import org.codehaus.jparsec.*;
import org.codehaus.jparsec.functors.*;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.ArrayList;


public class ResultHandler {

    enum BinaryOperator implements Binary<DocInfoList> {
        AND {
            public DocInfoList map(DocInfoList a, DocInfoList b) {
                
                DocInfoList out = a.and(b);
                return out;
            }
        },
        OR {
            public DocInfoList map(DocInfoList a, DocInfoList b) {

                DocInfoList out = a.or(b);
                return out;
            }
        }
    }

    enum UnaryOperator implements Unary<DocInfoList> {
        NEG {
            public DocInfoList map(DocInfoList n) {
                return n.not();
            }
        }
    }

    private HashMap<String, DocInfoList> fileInfoMaps = new HashMap<String, DocInfoList>();
    public void putFileInfoArray(String term, DocInfoList fileInfos) {
        this.fileInfoMaps.put(term, fileInfos);
    }

    final Parser<DocInfoList> IDENTIFIER = Terminals.Identifier.PARSER.map(new Map<String, DocInfoList>() {
        public DocInfoList map(String s) {

            return fileInfoMaps.get(s);
        }
    });

    private static final Terminals OPERATORS = Terminals.operators("AND", "OR", "-", "\"");

    static final Parser<Void> IGNORED = Scanners.WHITESPACES.skipMany();

    static final Parser<?> TOKENIZER =
        Parsers.or( OPERATORS.tokenizer(), Terminals.Identifier.TOKENIZER);

    static Parser<?> term(String... names) {
        return OPERATORS.token(names);
    }

    static final Parser<BinaryOperator> WHITESPACE_AND =
        term("AND", "OR", "\"").not().retn(BinaryOperator.AND);

    static <T> Parser<T> op(String name, T value) {
        return term(name).retn(value);
    }

    static Parser<DocInfoList> queryHandler(Parser<DocInfoList> atom) {
        Parser<DocInfoList> parser = new OperatorTable<DocInfoList>()
        .prefix(op("-", UnaryOperator.NEG), 30)
        .infixl(op("AND", BinaryOperator.AND).or(WHITESPACE_AND), 10)
        .infixl(op("OR", BinaryOperator.OR), 20)
        .build(atom);
        return parser;
    }

    public final Parser<DocInfoList> parser = queryHandler(IDENTIFIER).from(TOKENIZER, IGNORED);


}
