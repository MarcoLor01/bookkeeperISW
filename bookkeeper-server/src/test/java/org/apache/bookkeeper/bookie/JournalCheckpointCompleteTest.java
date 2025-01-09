package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.bookkeeper.bookie.BookieUtilJournal.writeV4Journal;

@RunWith(Parameterized.class)
public class JournalCheckpointCompleteTest {
    private final CheckPointStatus checkPointStatus;
    private Checkpoint checkpoint;
    private final boolean compact;
    private final byte[] KEY = "test".getBytes();
    private final Class<? extends Exception> expectedException;
    private BookieImpl bookie;
    private Journal journal;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                // TEST: checkpoint, compact -> Expected
                {CheckPointStatus.VALID, true, null},
                {CheckPointStatus.INVALID, true, null},
                {CheckPointStatus.NULL, true, null},
                {CheckPointStatus.VALID, false, null},
                {CheckPointStatus.INVALID, false, null},
                {CheckPointStatus.NULL, false, null},

        });
    }

    @Before
    public void setUp() throws Exception {

        File journalDirectory = createTempDirectory("bookie", "journal");
        File ledgerDirectory = createTempDirectory("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDirectory));
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDirectory));
        writeV4Journal(BookieImpl.getCurrentDirectory(journalDirectory), 5, KEY);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf
                .setJournalDirsName(new String[] {journalDirectory.getPath()})
                .setLedgerDirNames(new String[] { ledgerDirectory.getPath() })
                .setMetadataServiceUri(null);

        bookie = new TestBookieImpl(conf);
        this.journal = bookie.journals.get(0);

        setCheckpoint();
    }

    public JournalCheckpointCompleteTest(CheckPointStatus checkPointStatus, boolean compact,
                                  Class<? extends Exception> expectedException){
        this.checkPointStatus = checkPointStatus;
        this.compact = compact;
        this.expectedException = expectedException;
    }

    private void setCheckpoint(){
        switch (checkPointStatus){
            case NULL:
                checkpoint = null;
                break;
            case VALID:
                checkpoint = this.journal.newCheckpoint();
                break;
            case INVALID:
                checkpoint = CheckpointSource.DEFAULT.newCheckpoint();
                break;

        }
    }

    @Test
    public void test() {
        try {
            Assert.assertNotNull(journal);
            journal.checkpointComplete(checkpoint, compact);
            
            if (expectedException != null) {
                Assert.fail("Expected exception: " + expectedException + " but none was thrown.");
            }

        } catch(Exception e){
            Assert.assertEquals(expectedException, e.getClass());
        }
    }

    @After
    public void cleanUp() {
        if (bookie != null) {
            this.bookie.shutdown();
        }
        for (File dir : tempDirs) {
            FileUtils.deleteQuietly(dir);
        }
        tempDirs.clear();
    }

        List<File> tempDirs = new ArrayList<>();

    File createTempDirectory(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    public enum CheckPointStatus {
        VALID,
        INVALID,
        NULL,
    }


}
