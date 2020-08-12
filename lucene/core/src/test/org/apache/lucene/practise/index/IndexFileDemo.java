package org.apache.lucene.practise.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class IndexFileDemo {
    public static String PATH = "";
    private static final FieldType storedTextType = new FieldType(TextField.TYPE_STORED);

    @Test
    public void testIndex() throws IOException {
        Path indexPath = Paths.get("D:\\lucene\\index");
        Directory dir = FSDirectory.open(indexPath);
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);

        try (IndexWriter iw = new IndexWriter(dir, config)) {
            Document doc = new Document();
            doc.add(newTextField("fieldname", "abc", storedTextType));
            iw.addDocument(doc);
            iw.commit();

            doc.add(newTextField("fieldname1", "abcd", storedTextType));
            iw.addDocument(doc);
            iw.close();
        }
    }


    private Field newTextField(String name, String value, FieldType fieldType) {
        return new Field(name, (String) value, fieldType);
    }
}
