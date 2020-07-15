package org.impc.etl;

import lombok.extern.java.Log;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

@SpringBootApplication
@Log
public class MergeIndex implements CommandLineRunner {

    public static void main(String[] args) {
        log.info("STARTING THE APPLICATION");
        SpringApplication.run(MergeIndex.class, args);
        log.info("APPLICATION FINISHED");
    }


    @Override
    public void run(String... args) throws Exception {
        String dst = args[0];
        List<String> sources = asList(Arrays.copyOfRange(args, 1, args.length));

        try {
            merge(dst, sources);
        } catch (Exception ex) {
            log.severe("Merge failed!");
            log.severe(ex.toString());
            System.exit(1);
        }
    }


    /**
     * Copied and adapted from Lucene's own IndexMergeTool.
     */
    private void merge(String dst, List<String> sources) throws IOException {
        log.info(format("Merging %s into %s...", String.join(", ", sources), dst));

        FSDirectory mergedIndex = FSDirectory.open(Paths.get(dst));

        IndexWriter writer = new IndexWriter(mergedIndex,
                new IndexWriterConfig(null)
                        .setOpenMode(IndexWriterConfig.OpenMode.CREATE));

        Directory[] indexes = new Directory[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            log.info(format("Appending %s", sources.get(i)));
            indexes[i] = FSDirectory.open(Paths.get(sources.get(i)));
        }
        log.info("Adding indexes to writer");
        writer.addIndexes(indexes);
        log.info("Doing merge");
        writer.forceMerge(1);
        log.info("Committing");
        writer.commit();

        log.info(format("Done. The final index is at %s and contains documents.", dst));
        log.info("Feel free to delete all the temporary indexes.");

        writer.close();
    }
}
